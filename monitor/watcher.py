"""File-system monitor — watches configured directories for new media files,
runs the recognition pipeline headlessly, and records results into SQLite.

Automatically identifiable files are archived; unrecognizable ones are stored
with status ``pending_manual`` for the user to handle in the web UI.
"""

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Set

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from core.services.worker_context import WorkerContext
from db.database import SessionLocal
from db.scrape_models import MonitorFolder, ScrapeRecord, SymlinkRecord
from monitor.delete_sync import DeleteSyncService
from monitor.file_processor import process_file
from monitor.metadata_refresh import (
    record_to_dict as metadata_record_to_dict,
    refresh_record_metadata as refresh_record_metadata_impl,
    run_metadata_refresh_pass,
)
from monitor.scan_service import (
    enqueue_path,
    find_folder_for_path,
    is_symlink_export_path,
    poll_once,
    run_debounce_loop,
    scan_folder as run_scan_folder,
)
from utils.telegram_notify import NotificationBatcher
from utils.emby_notify import EmbyNotifier

logger = logging.getLogger(__name__)

# Debounce: wait this many seconds after last event before processing a file
_DEBOUNCE_SECONDS = 5.0

# Polling: scan folders every N seconds to catch network-written files
_POLL_INTERVAL_SECONDS = 30.0

# Metadata refresh: default interval (12 hours) and lookback (14 days)
_METADATA_REFRESH_DEFAULT_INTERVAL_HOURS = 12
_METADATA_REFRESH_DEFAULT_LOOKBACK_DAYS = 14


class _MediaHandler(FileSystemEventHandler):
    """watchdog handler that queues newly created / moved-in media files."""

    def __init__(self, watcher: "FolderWatcher"):
        super().__init__()
        self.watcher = watcher

    def on_created(self, event):
        if not event.is_directory:
            self.watcher.enqueue(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self.watcher.enqueue(event.dest_path)

    def on_deleted(self, event):
        if event.is_directory:
            self.watcher.on_dir_deleted(event.src_path)
        else:
            self.watcher.on_file_deleted(event.src_path)


class FolderWatcher:
    """Manages watchdog observers for all enabled MonitorFolder rows and
    processes new files through the recognition + archive pipeline.
    """

    def __init__(self, broadcast_fn=None):
        """
        Parameters
        ----------
        broadcast_fn : callable(dict), optional
            Called with a status-update dict whenever a ScrapeRecord changes.
            Typically wired to the WebSocket hub.
        """
        self._broadcast = broadcast_fn or (lambda d: None)
        self._observer = Observer()
        self._observer.daemon = True
        self._watches: Dict[int, object] = {}  # folder_id -> ObservedWatch
        self._pending: Dict[str, float] = {}  # path -> last event time
        self._pending_lock = threading.Lock()
        self._processed: Set[str] = set()
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scrape")
        self._symlink_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="symlink-export")
        self._dir_gate = threading.Condition()
        self._active_dirs: Set[str] = set()
        self._running = False
        self._worker_ctx: Optional[WorkerContext] = None
        self._debounce_thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._metadata_refresh_thread: Optional[threading.Thread] = None
        self._symlink_export_paths: Set[str] = set()
        self._tg_batcher = NotificationBatcher(
            cfg_getter=lambda: self._worker_ctx._cfg if self._worker_ctx else {}
        )
        self._emby_notifier = EmbyNotifier(
            cfg_getter=lambda: self._worker_ctx._cfg if self._worker_ctx else {}
        )
        self._delete_sync = DeleteSyncService(
            self,
            record_to_dict=_record_to_dict,
            symlink_record_to_dict=_symlink_record_to_dict,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if self._running:
            return
        self._running = True
        self._worker_ctx = WorkerContext()
        self._refresh_pool_workers()
        self._observer = Observer()
        self._observer.daemon = True
        self._observer.start()
        self._debounce_thread = threading.Thread(target=self._debounce_loop, daemon=True)
        self._debounce_thread.start()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        self._metadata_refresh_thread = threading.Thread(
            target=self._metadata_refresh_loop, daemon=True
        )
        self._metadata_refresh_thread.start()
        self._sync_watches()
        logger.info("FolderWatcher started")

    def stop(self):
        self._running = False
        try:
            self._observer.stop()
            self._observer.join(timeout=3)
        except Exception:
            pass
        self._watches.clear()
        self._pool.shutdown(wait=False)
        self._symlink_pool.shutdown(wait=False)
        logger.info("FolderWatcher stopped")

    def _desired_pool_workers(self) -> int:
        if self._worker_ctx:
            return self._worker_ctx._get_preview_workers()
        return 1

    def _desired_symlink_pool_workers(self) -> int:
        if self._worker_ctx:
            return self._worker_ctx._get_symlink_export_workers()
        return 3

    def _refresh_pool_workers(self):
        desired = self._desired_pool_workers()
        current = getattr(self._pool, "_max_workers", None)
        if current != desired:
            old_pool = self._pool
            self._pool = ThreadPoolExecutor(max_workers=desired, thread_name_prefix="scrape")
            try:
                old_pool.shutdown(wait=False)
            except Exception:
                pass

        desired_symlink = self._desired_symlink_pool_workers()
        current_symlink = getattr(self._symlink_pool, "_max_workers", None)
        if current_symlink != desired_symlink:
            old_symlink_pool = self._symlink_pool
            self._symlink_pool = ThreadPoolExecutor(max_workers=desired_symlink, thread_name_prefix="symlink-export")
            try:
                old_symlink_pool.shutdown(wait=False)
            except Exception:
                pass

    def reload_runtime_config(self):
        if self._worker_ctx:
            self._worker_ctx.reload_config()
            self._refresh_pool_workers()

    def _acquire_dir_slot(self, path: str) -> str:
        dir_key = os.path.normcase(os.path.normpath(os.path.dirname(path) or path))
        with self._dir_gate:
            while dir_key in self._active_dirs:
                self._dir_gate.wait(timeout=0.5)
            self._active_dirs.add(dir_key)
        return dir_key

    def _release_dir_slot(self, dir_key: str):
        with self._dir_gate:
            self._active_dirs.discard(dir_key)
            self._dir_gate.notify_all()

    def _sync_watches(self):
        """Synchronize watchdog watches with the database."""
        db = SessionLocal()
        try:
            folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
            # Cache symlink_export folder paths for enqueue bypass
            self._symlink_export_paths = {
                os.path.normpath(f.path) for f in folders
                if getattr(f, 'organize_mode', 'move') == 'symlink_export'
            }
            active_ids = set()
            for f in folders:
                active_ids.add(f.id)
                if f.id not in self._watches and os.path.isdir(f.path):
                    try:
                        w = self._observer.schedule(
                            _MediaHandler(self), f.path, recursive=True
                        )
                        self._watches[f.id] = w
                        logger.info(f"Watching: {f.path}")
                    except Exception as e:
                        logger.error(f"Failed to watch {f.path}: {e}")

            # Remove watches for disabled / deleted folders
            for fid in list(self._watches):
                if fid not in active_ids:
                    try:
                        self._observer.unschedule(self._watches[fid])
                    except Exception:
                        pass
                    del self._watches[fid]
        finally:
            db.close()

    def refresh(self):
        """Called after monitor folder CRUD to resync watches."""
        self._sync_watches()

    # ------------------------------------------------------------------
    # Deletion sync
    # ------------------------------------------------------------------

    def on_file_deleted(self, path: str):
        """watchdog 回调：监控路径内有文件被删除时触发，提交到线程池处理。"""
        norm = os.path.normpath(path)
        self._pool.submit(self._delete_sync.handle_file_deleted, norm)

    def on_dir_deleted(self, path: str):
        """watchdog 回调：监控路径内有目录被删除时触发，提交到线程池处理。"""
        norm = os.path.normpath(path)
        self._pool.submit(self._delete_sync.handle_dir_deleted, norm)

    # ------------------------------------------------------------------
    # Enqueue / debounce
    # ------------------------------------------------------------------

    def _is_symlink_export_path(self, path: str) -> bool:
        return is_symlink_export_path(self, path)

    def enqueue(self, path: str):
        """Called by the watchdog handler for each new file event."""
        enqueue_path(self, path)

    def _debounce_loop(self):
        run_debounce_loop(self, _DEBOUNCE_SECONDS)

    def _poll_loop(self):
        """Periodically scan all enabled folders for new files not yet recorded.
        This catches files written over the network where watchdog events are not delivered.
        """
        while self._running:
            time.sleep(_POLL_INTERVAL_SECONDS)
            if not self._running:
                break
            try:
                self._poll_once()
            except Exception as e:
                logger.error(f"Poll error: {e}")

    def _poll_once(self):
        """Single pass: walk enabled folders and enqueue any file not yet in ScrapeRecord."""
        poll_once(self)

    # ------------------------------------------------------------------
    # Metadata refresh patrol
    # ------------------------------------------------------------------

    def _metadata_refresh_loop(self):
        """Periodically scan recent successful records and refresh incomplete metadata."""
        # Wait a bit before the first run to let the server fully start
        time.sleep(60)
        while self._running:
            cfg = self._worker_ctx._cfg if self._worker_ctx else {}
            enabled = cfg.get("metadata_refresh_enabled", True)
            interval_hours = cfg.get(
                "metadata_refresh_interval_hours",
                _METADATA_REFRESH_DEFAULT_INTERVAL_HOURS,
            )
            interval_seconds = max(1800, interval_hours * 3600)  # min 30 min

            if enabled:
                try:
                    self._refresh_incomplete_records()
                except Exception as e:
                    logger.error(f"Metadata refresh error: {e}")

            # Sleep in small increments so we can exit quickly on stop
            slept = 0.0
            while slept < interval_seconds and self._running:
                time.sleep(min(30.0, interval_seconds - slept))
                slept += 30.0

    def _refresh_incomplete_records(self):
        """Single pass: find recent incomplete records and refresh their metadata."""
        if not self._worker_ctx:
            return

        cfg = self._worker_ctx._cfg if self._worker_ctx else {}
        lookback_days = cfg.get(
            "metadata_refresh_lookback_days",
            _METADATA_REFRESH_DEFAULT_LOOKBACK_DAYS,
        )
        run_metadata_refresh_pass(
            self._worker_ctx,
            lookback_days=lookback_days,
            running_check=lambda: self._running,
            broadcast_fn=self._broadcast,
        )

    def _refresh_single_record(self, record, db) -> bool:
        """Refresh metadata for a single ScrapeRecord. Returns True if updated."""
        return refresh_record_metadata(record, db, self._worker_ctx, self._broadcast)

    # ------------------------------------------------------------------
    # Full scan
    # ------------------------------------------------------------------

    def scan_folder(self, folder_id: int):
        """Manually trigger a full scan of one monitored folder."""
        run_scan_folder(self, folder_id)

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _find_folder(self, path: str, db) -> Optional[MonitorFolder]:
        """Find the MonitorFolder that owns *path*."""
        return find_folder_for_path(path, db)

    def _process_file(self, path: str):
        process_file(self, path)


def _delete_per_file_sidecars(file_path: str):
    from monitor.delete_sync import delete_per_file_sidecars

    return delete_per_file_sidecars(file_path)


# ---------------------------------------------------------------------------
# Metadata refresh — re-fetch from TMDB/BGM and update NFO/images
# ---------------------------------------------------------------------------

def refresh_record_metadata(record, db, worker_ctx, broadcast_fn=None) -> bool:
    return refresh_record_metadata_impl(record, db, worker_ctx, broadcast_fn)


# Windows / macOS auto-generated metadata files that should be ignored when
# deciding whether a directory is "effectively empty".
def _dir_real_entries(dir_path: str) -> list[str]:
    from monitor.delete_sync import dir_real_entries

    return dir_real_entries(dir_path)


def _remove_empty_dirs(start_dir: str, stop_at: Optional[str] = None):
    from monitor.delete_sync import remove_empty_dirs

    return remove_empty_dirs(start_dir, stop_at=stop_at)


def _symlink_record_to_dict(r: SymlinkRecord) -> dict:
    return {
        "id": r.id,
        "folder_id": r.folder_id,
        "original_path": r.original_path,
        "link_path": r.link_path,
        "status": r.status,
        "error_msg": r.error_msg,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


def _record_to_dict(record: ScrapeRecord) -> dict:
    return metadata_record_to_dict(record)
