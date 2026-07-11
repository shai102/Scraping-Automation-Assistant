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
from typing import Dict, Optional, Set, TYPE_CHECKING

from watchdog.observers import Observer

from db.database import SessionLocal
from db.scrape_models import MonitorFolder, ScrapeRecord, SymlinkRecord
from monitor.delete_sync import DeleteSyncService
from monitor.file_processor import process_file
from monitor.metadata_refresh import (
    record_to_dict as metadata_record_to_dict,
    refresh_record_metadata as refresh_record_metadata_impl,
)
from monitor.scan_service import (
    enqueue_path,
    find_folder_for_path,
    is_symlink_export_path,
    poll_once,
    run_debounce_loop,
    scan_folder as run_scan_folder,
)
from monitor.watcher_lifecycle import (
    refresh_pool_workers as refresh_pool_workers_impl,
    start_watcher,
    stop_watcher,
    sync_watches as sync_watches_impl,
)
from monitor.watcher_metadata import (
    metadata_refresh_loop,
    refresh_incomplete_records,
    refresh_single_record,
)
from utils.telegram_notify import NotificationBatcher
from utils.emby_notify import EmbyNotifier

if TYPE_CHECKING:
    from core.services.worker_context import WorkerContext

logger = logging.getLogger(__name__)

# Debounce: wait this many seconds after last event before processing a file
_DEBOUNCE_SECONDS = 5.0

# Polling: scan folders every N seconds to catch network-written files
_POLL_INTERVAL_SECONDS = 30.0
_POLL_MAX_ENQUEUE_PER_PASS = 500

# Metadata refresh: default interval (12 hours) and lookback (14 days)
_METADATA_REFRESH_DEFAULT_INTERVAL_HOURS = 12
_METADATA_REFRESH_DEFAULT_LOOKBACK_DAYS = 14


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
        self._pending_task_ids: Dict[str, int] = {}
        self._pending_task_types: Dict[str, str] = {}
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
        self._maintenance_thread: Optional[threading.Thread] = None
        self._started_at = None
        self._last_poll_at = None
        self._last_success_at = None
        self._last_maintenance_at = None
        self._symlink_export_paths: Set[str] = set()
        self._poll_max_enqueue_per_pass = _POLL_MAX_ENQUEUE_PER_PASS
        self._poll_use_scan_state = True
        self._tg_batcher = NotificationBatcher(
            cfg_getter=lambda: self._worker_ctx._cfg if self._worker_ctx else {}
        )
        self._emby_notifier = EmbyNotifier(
            cfg_getter=lambda: self._worker_ctx._cfg if self._worker_ctx else {}
        )
        self._session_factory = SessionLocal
        self._folder_model = MonitorFolder
        self._delete_sync = DeleteSyncService(
            self,
            record_to_dict=_record_to_dict,
            symlink_record_to_dict=_symlink_record_to_dict,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        start_watcher(self)

    def stop(self):
        stop_watcher(self)

    def _desired_pool_workers(self) -> int:
        return getattr(self._pool, "_max_workers", 1)

    def _desired_symlink_pool_workers(self) -> int:
        return getattr(self._symlink_pool, "_max_workers", 1)

    def _refresh_pool_workers(self):
        refresh_pool_workers_impl(self)

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
        sync_watches_impl(self)

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

    def enqueue(self, path: str, **kwargs):
        """Called by the watchdog handler for each new file event."""
        return enqueue_path(self, path, **kwargs)

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
        metadata_refresh_loop(
            self,
            default_interval_hours=_METADATA_REFRESH_DEFAULT_INTERVAL_HOURS,
            default_lookback_days=_METADATA_REFRESH_DEFAULT_LOOKBACK_DAYS,
        )

    def _refresh_incomplete_records(self):
        refresh_incomplete_records(self, default_lookback_days=_METADATA_REFRESH_DEFAULT_LOOKBACK_DAYS)

    def _refresh_single_record(self, record, db) -> bool:
        return refresh_single_record(self, record, db)

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

    def _process_file(self, path: str, task_id: int | None = None):
        process_file(self, path, task_id=task_id)


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
