"""File-system monitor — watches configured directories for new media files,
runs the recognition pipeline headlessly, and records results into SQLite.

Automatically identifiable files are archived; unrecognizable ones are stored
with status ``pending_manual`` for the user to handle in the web UI.
"""

import json
import logging
import os
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Set

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from core.models.media_item import MediaItem
from core.services.worker_context import WorkerContext
from core.workers.task_runner import process_task as _process_task
from db.database import SessionLocal
from db.scrape_models import MonitorFolder, ScrapeRecord
from utils.telegram_notify import NotificationBatcher

logger = logging.getLogger(__name__)

# Debounce: wait this many seconds after last event before processing a file
_DEBOUNCE_SECONDS = 5.0

# Polling: scan folders every N seconds to catch network-written files
_POLL_INTERVAL_SECONDS = 30.0


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
        self._pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="scrape")
        self._running = False
        self._worker_ctx: Optional[WorkerContext] = None
        self._debounce_thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._tg_batcher = NotificationBatcher(
            cfg_getter=lambda: self._worker_ctx._cfg if self._worker_ctx else {}
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if self._running:
            return
        self._running = True
        self._worker_ctx = WorkerContext()
        self._observer = Observer()
        self._observer.daemon = True
        self._observer.start()
        self._debounce_thread = threading.Thread(target=self._debounce_loop, daemon=True)
        self._debounce_thread.start()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        self._sync_watches()
        logger.info("FolderWatcher started")

    def stop(self):
        self._running = False
        try:
            self._observer.stop()
            self._observer.join(timeout=3)
        except Exception:
            pass
        self._pool.shutdown(wait=False)
        logger.info("FolderWatcher stopped")

    def _sync_watches(self):
        """Synchronize watchdog watches with the database."""
        db = SessionLocal()
        try:
            folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
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
    # Enqueue / debounce
    # ------------------------------------------------------------------

    def enqueue(self, path: str):
        """Called by the watchdog handler for each new file event."""
        # Quick extension filter
        if not self._worker_ctx:
            return
        exts = self._worker_ctx.get_media_exts()
        if not path.lower().endswith(exts):
            return
        norm = os.path.normpath(path)
        with self._pending_lock:
            if norm in self._processed:
                return
            self._pending[norm] = time.time()

    def _debounce_loop(self):
        while self._running:
            time.sleep(1.0)
            now = time.time()
            ready = []
            with self._pending_lock:
                for p, t in list(self._pending.items()):
                    if now - t >= _DEBOUNCE_SECONDS:
                        ready.append(p)
                        del self._pending[p]
            for p in ready:
                with self._pending_lock:
                    if p in self._processed:
                        continue
                    self._processed.add(p)
                self._pool.submit(self._process_file, p)

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
        if not self._worker_ctx:
            return
        exts = self._worker_ctx.get_media_exts()
        db = SessionLocal()
        try:
            folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
            for folder in folders:
                if not os.path.isdir(folder.path):
                    continue
                for dirpath, _, filenames in os.walk(folder.path):
                    for fn in filenames:
                        if not fn.lower().endswith(exts):
                            continue
                        full = os.path.normpath(os.path.join(dirpath, fn))
                        with self._pending_lock:
                            if full in self._processed or full in self._pending:
                                continue
                        # Check DB — already processed?
                        existing = db.query(ScrapeRecord).filter(
                            ScrapeRecord.original_path == full
                        ).first()
                        if existing:
                            with self._pending_lock:
                                self._processed.add(full)
                            continue
                        # New file — enqueue via debounce
                        with self._pending_lock:
                            self._pending[full] = time.time()
                        logger.debug(f"Poll found new file: {full}")
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Full scan
    # ------------------------------------------------------------------

    def scan_folder(self, folder_id: int):
        """Manually trigger a full scan of one monitored folder."""
        db = SessionLocal()
        try:
            folder = db.query(MonitorFolder).get(folder_id)
            if not folder or not os.path.isdir(folder.path):
                return
            exts = self._worker_ctx.get_media_exts() if self._worker_ctx else ()
            for dirpath, _, filenames in os.walk(folder.path):
                for fn in filenames:
                    if fn.lower().endswith(exts):
                        full = os.path.normpath(os.path.join(dirpath, fn))
                        # Skip already-recorded files
                        existing = db.query(ScrapeRecord).filter(
                            ScrapeRecord.original_path == full
                        ).first()
                        if existing:
                            continue
                        with self._pending_lock:
                            if full not in self._processed:
                                self._processed.add(full)
                        self._pool.submit(self._process_file, full)
        finally:
            db.close()

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _find_folder(self, path: str, db) -> Optional[MonitorFolder]:
        """Find the MonitorFolder that owns *path*."""
        folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
        norm = os.path.normpath(path)
        best = None
        for f in folders:
            fp = os.path.normpath(f.path)
            if norm.startswith(fp + os.sep) or norm == fp:
                if best is None or len(fp) > len(os.path.normpath(best.path)):
                    best = f
        return best

    def _process_file(self, path: str):
        """Run the full recognition + archive pipeline for a single file."""
        if not os.path.isfile(path):
            return

        db = SessionLocal()
        try:
            # Check for duplicate
            existing = db.query(ScrapeRecord).filter(ScrapeRecord.original_path == path).first()
            if existing:
                return

            folder = self._find_folder(path, db)

            # Decimal episode (e.g. 4.5) = \u603b\u96c6\u7bc7 \u2014 skip
            from utils.helpers import is_decimal_episode
            pure_name = os.path.splitext(os.path.basename(path))[0]
            if is_decimal_episode(pure_name):
                logger.info(f"\u8df3\u8fc7\u5c0f\u6570\u96c6\uff08\u603b\u96c6\u7bc7\uff09: {path}")
                record = ScrapeRecord(
                    folder_id=folder.id if folder else None,
                    original_path=path,
                    original_name=os.path.basename(path),
                    status="skipped",
                    error_msg="\u5c0f\u6570\u96c6\uff08\u603b\u96c6\u7bc7\uff09\uff0c\u5df2\u8df3\u8fc7",
                )
                db.add(record)
                db.commit()
                db.refresh(record)
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                return

            # Create record
            record = ScrapeRecord(
                folder_id=folder.id if folder else None,
                original_path=path,
                original_name=os.path.basename(path),
                status="processing",
            )
            db.add(record)
            db.commit()
            db.refresh(record)
            self._broadcast({"type": "record_update", "data": _record_to_dict(record)})

            # Build a per-call WorkerContext to avoid concurrent mutation of shared state.
            # Each thread gets its own copy of the config; the global _worker_ctx is only
            # used for extension filtering (enqueue / _poll_once).
            if not self._worker_ctx:
                return
            ctx = WorkerContext(config=dict(self._worker_ctx._cfg))

            # Apply folder-level overrides onto this thread-local ctx
            if folder:
                if folder.target_root:
                    ctx.target_root.set(folder.target_root)
                if folder.data_source:
                    ctx.source_var.set(folder.data_source)
                if folder.media_type == "movie":
                    ctx.media_type_override.set("电影")
                elif folder.media_type == "tv":
                    ctx.media_type_override.set("电视剧")
                else:
                    ctx.media_type_override.set("自动判断")

            item = MediaItem(
                id=str(uuid.uuid4()),
                path=path,
                dir=os.path.dirname(path),
                old_name=os.path.basename(path),
                ext=os.path.splitext(path)[1],
            )
            ctx.file_list = [item]

            # === Recognition (process_task) ===
            try:
                _process_task(ctx, 0)
            except Exception as e:
                logger.error(f"Recognition failed for {path}: {e}")
                record.status = "failed"
                record.error_msg = str(e)[:500]
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                return

            # Check recognition result
            tid = (item.metadata or {}).get("id", "None")
            if tid == "None" or not item.new_name_only:
                record.status = "pending_manual"
                record.matched_title = (item.metadata or {}).get("title")
                record.error_msg = "无法自动识别"
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                return

            # === Archive (move + sidecar) ===
            try:
                target = item.full_target or os.path.join(item.dir, item.new_name_only)
                target_dir = os.path.dirname(target)
                if target_dir:
                    os.makedirs(target_dir, exist_ok=True)

                if os.path.normcase(item.path) != os.path.normcase(target):
                    if os.path.exists(target):
                        record.status = "failed"
                        record.error_msg = "目标文件已存在"
                        db.commit()
                        self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                        return
                    src_dir = os.path.dirname(item.path)
                    shutil.move(item.path, target)
                    item.path = target
                    # Remove empty parent directories up to (but not including) the monitored root
                    watch_root = os.path.normpath(folder.path) if folder else None
                    _remove_empty_dirs(src_dir, stop_at=watch_root)

                # Write sidecar files
                ctx._write_sidecar_files(item, target)

                record.status = "success"
                record.matched_title = (item.metadata or {}).get("title")
                record.matched_id = str(tid)
                record.matched_provider = (item.metadata or {}).get("provider")
                record.target_path = target
                record.metadata_json = json.dumps(item.metadata or {}, ensure_ascii=False)
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})
                logger.info(f"Archived: {os.path.basename(path)} -> {target}")

                # Telegram batch notification
                try:
                    self._tg_batcher.add(
                        folder.id if folder else 0,
                        os.path.basename(folder.path) if folder else "",
                        item,
                    )
                except Exception as _tg_err:
                    logger.debug(f"TG 通知排队失败: {_tg_err}")

            except Exception as e:
                logger.error(f"Archive failed for {path}: {e}")
                record.status = "failed"
                record.error_msg = str(e)[:500]
                db.commit()
                self._broadcast({"type": "record_update", "data": _record_to_dict(record)})

        except Exception as e:
            logger.error(f"Unexpected error processing {path}: {e}")
        finally:
            db.close()


def _remove_empty_dirs(start_dir: str, stop_at: Optional[str] = None):
    """Walk upward from *start_dir* removing each directory that is empty.
    Stops before removing *stop_at* (the monitored root folder itself).
    """
    current = os.path.normpath(start_dir)
    while True:
        if stop_at and os.path.normcase(current) == os.path.normcase(stop_at):
            break  # never remove the watch root itself
        parent = os.path.dirname(current)
        if parent == current:
            break  # filesystem root
        try:
            if os.path.isdir(current) and not os.listdir(current):
                os.rmdir(current)
                logger.debug(f"Removed empty dir: {current}")
            else:
                break  # directory not empty, stop climbing
        except Exception as e:
            logger.warning(f"Could not remove dir {current}: {e}")
            break
        current = parent


def _record_to_dict(record: ScrapeRecord) -> dict:
    return {
        "id": record.id,
        "folder_id": record.folder_id,
        "original_path": record.original_path,
        "original_name": record.original_name,
        "status": record.status,
        "matched_title": record.matched_title,
        "matched_id": record.matched_id,
        "matched_provider": record.matched_provider,
        "target_path": record.target_path,
        "error_msg": record.error_msg,
        "created_at": record.created_at.isoformat() if record.created_at else None,
        "updated_at": record.updated_at.isoformat() if record.updated_at else None,
    }
