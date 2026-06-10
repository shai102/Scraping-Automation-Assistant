import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from core.services.worker_context import WorkerContext
from monitor.record_state import reset_stale_processing_records
from monitor.scan_service import restore_queued_tasks
from monitor.task_queue import recover_stale_running_tasks


logger = logging.getLogger(__name__)


class MediaHandler(FileSystemEventHandler):
    def __init__(self, watcher):
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


def desired_pool_workers(watcher) -> int:
    if watcher._worker_ctx:
        return watcher._worker_ctx._get_preview_workers()
    return 1


def desired_symlink_pool_workers(watcher) -> int:
    if watcher._worker_ctx:
        return watcher._worker_ctx._get_symlink_export_workers()
    return 3


def refresh_pool_workers(watcher):
    desired = desired_pool_workers(watcher)
    current = getattr(watcher._pool, "_max_workers", None)
    if current != desired:
        old_pool = watcher._pool
        watcher._pool = ThreadPoolExecutor(max_workers=desired, thread_name_prefix="scrape")
        try:
            old_pool.shutdown(wait=False)
        except Exception:
            pass

    desired_symlink = desired_symlink_pool_workers(watcher)
    current_symlink = getattr(watcher._symlink_pool, "_max_workers", None)
    if current_symlink != desired_symlink:
        old_symlink_pool = watcher._symlink_pool
        watcher._symlink_pool = ThreadPoolExecutor(
            max_workers=desired_symlink, thread_name_prefix="symlink-export"
        )
        try:
            old_symlink_pool.shutdown(wait=False)
        except Exception:
            pass


def sync_watches(watcher):
    db = watcher._session_factory()
    try:
        folders = db.query(watcher._folder_model).filter(watcher._folder_model.enabled.is_(True)).all()
        watcher._symlink_export_paths = {
            os.path.normpath(folder.path)
            for folder in folders
            if getattr(folder, "organize_mode", "move") == "symlink_export"
        }
        active_ids = set()
        for folder in folders:
            active_ids.add(folder.id)
            if folder.id not in watcher._watches and os.path.isdir(folder.path):
                try:
                    watch = watcher._observer.schedule(MediaHandler(watcher), folder.path, recursive=True)
                    watcher._watches[folder.id] = watch
                    logger.info(f"Watching: {folder.path}")
                except Exception as err:
                    logger.error(f"Failed to watch {folder.path}: {err}")

        for folder_id in list(watcher._watches):
            if folder_id not in active_ids:
                try:
                    watcher._observer.unschedule(watcher._watches[folder_id])
                except Exception:
                    pass
                del watcher._watches[folder_id]
    finally:
        db.close()


def start_watcher(watcher):
    if watcher._running:
        return
    watcher._running = True
    watcher._worker_ctx = WorkerContext()
    db = watcher._session_factory()
    try:
        reset_stale_processing_records(db)
        recover_stale_running_tasks(db, stale_minutes=0)
    finally:
        db.close()
    refresh_pool_workers(watcher)
    watcher._observer = Observer()
    watcher._observer.daemon = True
    watcher._observer.start()
    watcher._debounce_thread = threading.Thread(target=watcher._debounce_loop, daemon=True)
    watcher._debounce_thread.start()
    watcher._poll_thread = threading.Thread(target=watcher._poll_loop, daemon=True)
    watcher._poll_thread.start()
    watcher._metadata_refresh_thread = threading.Thread(target=watcher._metadata_refresh_loop, daemon=True)
    watcher._metadata_refresh_thread.start()
    sync_watches(watcher)
    restored = restore_queued_tasks(watcher)
    if restored:
        logger.info("Restored queued watcher tasks: %s", restored)
    logger.info("FolderWatcher started")


def stop_watcher(watcher):
    watcher._running = False
    try:
        watcher._observer.stop()
        watcher._observer.join(timeout=3)
    except Exception:
        pass
    watcher._watches.clear()
    watcher._pool.shutdown(wait=False)
    watcher._symlink_pool.shutdown(wait=False)
    logger.info("FolderWatcher stopped")
