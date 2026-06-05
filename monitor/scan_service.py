"""Folder scanning and debounce helpers for FolderWatcher."""

import logging
import os
import time

from db.database import SessionLocal
from db.scrape_models import MonitorFolder, ScrapeRecord, SymlinkRecord
from monitor.record_state import (
    is_already_scraped,
    scrape_record_needs_repair,
    symlink_record_consumed_downstream,
    symlink_record_needs_repair,
    symlink_source_consumed_downstream,
)

logger = logging.getLogger(__name__)


def find_folder_for_path(path: str, db) -> MonitorFolder | None:
    """Return the enabled MonitorFolder that most specifically owns *path*."""
    folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
    norm = os.path.normpath(path)
    best = None
    for folder in folders:
        folder_path = os.path.normpath(folder.path)
        if norm.startswith(folder_path + os.sep) or norm == folder_path:
            if best is None or len(folder_path) > len(os.path.normpath(best.path)):
                best = folder
    return best


def is_symlink_export_path(watcher, path: str) -> bool:
    norm = os.path.normpath(path)
    return any(norm.startswith(root + os.sep) or norm == root for root in watcher._symlink_export_paths)


def enqueue_path(watcher, path: str):
    """Queue a new file event for delayed processing."""
    if not watcher._worker_ctx:
        return
    norm = os.path.normpath(path)
    if not is_symlink_export_path(watcher, path):
        exts = watcher._worker_ctx.get_media_exts()
        if not path.lower().endswith(exts):
            return
    with watcher._pending_lock:
        if norm in watcher._processed:
            return
        watcher._pending[norm] = time.time()


def run_debounce_loop(watcher, debounce_seconds: float):
    while watcher._running:
        time.sleep(1.0)
        now = time.time()
        ready = []
        with watcher._pending_lock:
            for path, last_event_at in list(watcher._pending.items()):
                if now - last_event_at >= debounce_seconds:
                    ready.append(path)
                    del watcher._pending[path]
        for path in ready:
            with watcher._pending_lock:
                if path in watcher._processed:
                    continue
                watcher._processed.add(path)
            target_pool = watcher._symlink_pool if is_symlink_export_path(watcher, path) else watcher._pool
            target_pool.submit(watcher._process_file, path)
            time.sleep(0.03 if target_pool is watcher._symlink_pool else 0.1)


def _load_recorded_paths(folder, db, worker_ctx) -> set[str]:
    if getattr(folder, "organize_mode", "move") == "symlink_export":
        recorded = set()
        rows = db.query(SymlinkRecord).filter(SymlinkRecord.folder_id == folder.id).all()
        for row in rows:
            if symlink_record_needs_repair(row) and not symlink_record_consumed_downstream(row, db, worker_ctx):
                continue
            if row.original_path:
                recorded.add(row.original_path)
        return recorded

    recorded = set()
    rows = db.query(ScrapeRecord).filter(ScrapeRecord.folder_id == folder.id).all()
    for row in rows:
        if scrape_record_needs_repair(row, worker_ctx):
            continue
        for recorded_path in (row.original_path, row.target_path):
            if recorded_path:
                recorded.add(recorded_path)
    return recorded


def _iter_folder_candidates(watcher, folder, *, exts, recorded: set[str], mark_skipped: bool, db):
    is_symlink_export = getattr(folder, "organize_mode", "move") == "symlink_export"
    skip_scraped = getattr(folder, "skip_if_scraped", False) and not is_symlink_export
    sub_exts = watcher._worker_ctx.get_sub_audio_exts() if watcher._worker_ctx else ()

    for dirpath, _, filenames in os.walk(folder.path):
        for filename in filenames:
            if not is_symlink_export and not filename.lower().endswith(exts):
                continue

            full = os.path.normpath(os.path.join(dirpath, filename))

            if skip_scraped and is_already_scraped(full, sub_exts):
                with watcher._pending_lock:
                    watcher._processed.add(full)
                if mark_skipped:
                    skip_rec = ScrapeRecord(
                        folder_id=folder.id,
                        original_path=full,
                        original_name=os.path.basename(full),
                        status="skipped",
                        error_msg="已有元数据（.nfo），跳过刮削",
                    )
                    db.add(skip_rec)
                    db.commit()
                continue

            if full in recorded:
                with watcher._pending_lock:
                    watcher._processed.add(full)
                continue

            if is_symlink_export and symlink_source_consumed_downstream(folder, full, db, watcher._worker_ctx):
                with watcher._pending_lock:
                    watcher._processed.add(full)
                continue

            yield full


def poll_once(watcher):
    """Walk enabled folders and enqueue any file not yet recorded."""
    if not watcher._worker_ctx:
        return
    exts = watcher._worker_ctx.get_media_exts()
    db = SessionLocal()
    try:
        folders = db.query(MonitorFolder).filter(MonitorFolder.enabled == True).all()
        for folder in folders:
            if not os.path.isdir(folder.path):
                continue
            recorded = _load_recorded_paths(folder, db, watcher._worker_ctx)
            for full in _iter_folder_candidates(
                watcher,
                folder,
                exts=exts,
                recorded=recorded,
                mark_skipped=False,
                db=db,
            ):
                with watcher._pending_lock:
                    if full in watcher._processed or full in watcher._pending:
                        continue
                    watcher._pending[full] = time.time()
                logger.debug(f"Poll found new file: {full}")
    finally:
        db.close()


def scan_folder(watcher, folder_id: int):
    """Manually trigger a full scan of one monitored folder."""
    db = SessionLocal()
    try:
        folder = db.query(MonitorFolder).get(folder_id)
        if not folder or not os.path.isdir(folder.path):
            return
        exts = watcher._worker_ctx.get_media_exts() if watcher._worker_ctx else ()
        recorded = _load_recorded_paths(folder, db, watcher._worker_ctx)
        for full in _iter_folder_candidates(
            watcher,
            folder,
            exts=exts,
            recorded=recorded,
            mark_skipped=True,
            db=db,
        ):
            with watcher._pending_lock:
                if full not in watcher._processed:
                    watcher._processed.add(full)
            target_pool = watcher._symlink_pool if getattr(folder, "organize_mode", "move") == "symlink_export" else watcher._pool
            target_pool.submit(watcher._process_file, full)
            time.sleep(0.03 if target_pool is watcher._symlink_pool else 0.1)
    finally:
        db.close()
