"""Folder scanning and debounce helpers for FolderWatcher."""

import logging
import os
import time
import datetime

from db.database import SessionLocal
from db.scrape_models import FolderScanState, MonitorFolder, ScrapeRecord, SymlinkRecord
from monitor.record_state import (
    is_already_scraped,
    scrape_record_needs_repair,
    symlink_record_consumed_downstream,
    symlink_record_needs_repair,
    symlink_source_consumed_downstream,
)
from monitor.task_queue import enqueue_task, finish_task_by_id, load_queued_tasks

logger = logging.getLogger(__name__)


def _norm_abs(path: str) -> str:
    return os.path.normcase(os.path.normpath(os.path.abspath(path)))


def _scan_skip_roots_for_folder(folder, all_folders) -> set[str]:
    folder_root = _norm_abs(folder.path)
    candidates = []
    for item in all_folders:
        if item is not folder:
            candidates.append(getattr(item, "path", ""))
        candidates.append(getattr(item, "target_root", ""))

    skip_roots = set()
    for candidate in candidates:
        candidate = str(candidate or "").strip()
        if not candidate:
            continue
        candidate_root = _norm_abs(candidate)
        if candidate_root != folder_root and candidate_root.startswith(folder_root + os.sep):
            skip_roots.add(candidate_root)
    return skip_roots


def _should_skip_scan_dir(path: str, skip_roots: set[str]) -> bool:
    norm_path = _norm_abs(path)
    return any(norm_path == root or norm_path.startswith(root + os.sep) for root in skip_roots)


def _dir_mtime_ns(path: str) -> int | None:
    try:
        return os.stat(path).st_mtime_ns
    except OSError:
        return None


def _load_scan_state(db, folder_id: int) -> dict[str, FolderScanState]:
    rows = db.query(FolderScanState).filter(FolderScanState.folder_id == folder_id).all()
    return {row.dir_key: row for row in rows}


def _mark_scan_state_clean(
    db,
    scan_state: dict[str, FolderScanState],
    folder_id: int,
    dirpath: str,
    dir_key: str,
    mtime_ns: int,
):
    row = scan_state.get(dir_key)
    if row is None:
        row = FolderScanState(
            folder_id=folder_id,
            dir_path=os.path.normpath(dirpath),
            dir_key=dir_key,
            mtime_ns=mtime_ns,
        )
        db.add(row)
        scan_state[dir_key] = row
    else:
        row.dir_path = os.path.normpath(dirpath)
        row.mtime_ns = mtime_ns


def find_folder_for_path(path: str, db) -> MonitorFolder | None:
    """Return the enabled MonitorFolder that most specifically owns *path*."""
    folders = db.query(MonitorFolder).filter(MonitorFolder.enabled.is_(True)).all()
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


def _task_type_for_folder(folder) -> str:
    return "symlink_export" if getattr(folder, "organize_mode", "move") == "symlink_export" else "scrape"


def _task_type_for_path(watcher, path: str, folder=None) -> str:
    if folder is not None:
        return _task_type_for_folder(folder)
    return "symlink_export" if is_symlink_export_path(watcher, path) else "scrape"


def _path_allowed_for_folder(watcher, path: str, folder=None) -> bool:
    if _task_type_for_path(watcher, path, folder) == "symlink_export":
        return True
    exts = watcher._worker_ctx.get_media_exts()
    return path.lower().endswith(exts)


def _submit_task(watcher, path: str, task_id: int | None = None, task_type: str | None = None):
    task_type = task_type or _task_type_for_path(watcher, path)
    target_pool = watcher._symlink_pool if task_type == "symlink_export" else watcher._pool
    target_pool.submit(watcher._process_file, path, task_id)
    time.sleep(0.03 if target_pool is watcher._symlink_pool else 0.1)


def enqueue_path(
    watcher,
    path: str,
    *,
    source: str = "watchdog",
    immediate: bool = False,
    force: bool = False,
    folder=None,
    db=None,
):
    """Queue a new file event for delayed processing."""
    if not watcher._worker_ctx:
        return None
    norm = os.path.normpath(path)

    with watcher._pending_lock:
        if force:
            watcher._processed.discard(norm)
            watcher._pending.pop(norm, None)
            getattr(watcher, "_pending_task_ids", {}).pop(norm, None)
            getattr(watcher, "_pending_task_types", {}).pop(norm, None)
        elif norm in watcher._processed:
            return None

    own_db = db is None
    db = db or SessionLocal()
    try:
        folder = folder or find_folder_for_path(norm, db)
        if not _path_allowed_for_folder(watcher, norm, folder):
            return None
        task_type = _task_type_for_path(watcher, norm, folder)
        task, _created = enqueue_task(
            db,
            norm,
            folder_id=getattr(folder, "id", None),
            task_type=task_type,
            source=source,
        )
        task_id = task.id
        task_status = task.status
    finally:
        if own_db:
            db.close()

    if task_status == "running" and not force:
        return task_id

    with watcher._pending_lock:
        if force:
            watcher._processed.discard(norm)
        elif norm in watcher._processed:
            return task_id

        if immediate:
            watcher._pending.pop(norm, None)
            getattr(watcher, "_pending_task_ids", {}).pop(norm, None)
            getattr(watcher, "_pending_task_types", {}).pop(norm, None)
            watcher._processed.add(norm)
        else:
            watcher._pending[norm] = time.time()
            getattr(watcher, "_pending_task_ids", {})[norm] = task_id
            getattr(watcher, "_pending_task_types", {})[norm] = task_type

    if immediate:
        _submit_task(watcher, norm, task_id, task_type)
    return task_id


def run_debounce_loop(watcher, debounce_seconds: float):
    while watcher._running:
        time.sleep(1.0)
        now = time.time()
        ready = []
        with watcher._pending_lock:
            for path, last_event_at in list(watcher._pending.items()):
                if now - last_event_at >= debounce_seconds:
                    task_id = getattr(watcher, "_pending_task_ids", {}).pop(path, None)
                    task_type = getattr(watcher, "_pending_task_types", {}).pop(path, None)
                    ready.append((path, task_id, task_type))
                    del watcher._pending[path]
        for path, task_id, task_type in ready:
            with watcher._pending_lock:
                if path in watcher._processed:
                    if task_id:
                        finish_task_by_id(task_id, "skipped", "本轮已处理，跳过重复任务")
                    continue
                watcher._processed.add(path)
            _submit_task(watcher, path, task_id, task_type)


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


def _iter_folder_candidates(
    watcher,
    folder,
    *,
    exts,
    recorded: set[str],
    mark_skipped: bool,
    db,
    skip_roots: set[str] | None = None,
    scan_state: dict[str, FolderScanState] | None = None,
    update_scan_state: bool = False,
):
    is_symlink_export = getattr(folder, "organize_mode", "move") == "symlink_export"
    skip_scraped = getattr(folder, "skip_if_scraped", False) and not is_symlink_export
    sub_exts = watcher._worker_ctx.get_sub_audio_exts() if watcher._worker_ctx else ()
    skip_roots = skip_roots or set()

    for dirpath, dirnames, filenames in os.walk(folder.path):
        if skip_roots:
            if _should_skip_scan_dir(dirpath, skip_roots):
                dirnames[:] = []
                continue
            dirnames[:] = [
                name
                for name in dirnames
                if not _should_skip_scan_dir(os.path.join(dirpath, name), skip_roots)
            ]

        mtime_ns = _dir_mtime_ns(dirpath)
        dir_key = _norm_abs(dirpath)
        if update_scan_state and scan_state is not None and mtime_ns is not None:
            previous = scan_state.get(dir_key)
            if previous and int(getattr(previous, "mtime_ns", 0) or 0) == mtime_ns:
                continue

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

        if update_scan_state and scan_state is not None and mtime_ns is not None:
            _mark_scan_state_clean(db, scan_state, folder.id, dirpath, dir_key, mtime_ns)


def poll_once(watcher):
    watcher._last_poll_at = datetime.datetime.now()
    """Walk enabled folders and enqueue any file not yet recorded."""
    if not watcher._worker_ctx:
        return 0
    exts = watcher._worker_ctx.get_media_exts()
    max_enqueue = getattr(watcher, "_poll_max_enqueue_per_pass", 500)
    queued = 0
    db = SessionLocal()
    try:
        folders = db.query(MonitorFolder).filter(MonitorFolder.enabled.is_(True)).all()
        for folder in folders:
            if not os.path.isdir(folder.path):
                continue
            recorded = _load_recorded_paths(folder, db, watcher._worker_ctx)
            skip_roots = _scan_skip_roots_for_folder(folder, folders)
            scan_state = (
                _load_scan_state(db, folder.id)
                if getattr(watcher, "_poll_use_scan_state", True)
                else None
            )
            for full in _iter_folder_candidates(
                watcher,
                folder,
                exts=exts,
                recorded=recorded,
                mark_skipped=False,
                db=db,
                skip_roots=skip_roots,
                scan_state=scan_state,
                update_scan_state=scan_state is not None,
            ):
                with watcher._pending_lock:
                    if full in watcher._processed or full in watcher._pending:
                        continue
                task, _created = enqueue_task(
                    db,
                    full,
                    folder_id=folder.id,
                    task_type=_task_type_for_folder(folder),
                    source="poll",
                )
                if task.status != "queued":
                    continue
                with watcher._pending_lock:
                    if full in watcher._processed or full in watcher._pending:
                        continue
                    watcher._pending[full] = time.time()
                    getattr(watcher, "_pending_task_ids", {})[full] = task.id
                    getattr(watcher, "_pending_task_types", {})[full] = task.task_type
                    queued += 1
                logger.debug(f"Poll found new file: {full}")
                if max_enqueue and queued >= max_enqueue:
                    logger.info("Poll enqueue limit reached: %s", max_enqueue)
                    db.commit()
                    return queued
            db.commit()
        return queued
    finally:
        db.close()


def scan_folder(watcher, folder_id: int):
    """Manually trigger a full scan of one monitored folder."""
    db = SessionLocal()
    try:
        folder = db.get(MonitorFolder, folder_id)
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
            enqueue_path(
                watcher,
                full,
                source="manual_scan",
                immediate=True,
                folder=folder,
                db=db,
            )
    finally:
        db.close()


def restore_queued_tasks(watcher, *, limit: int = 1000) -> int:
    """Load persisted queued tasks back into the debounce buffer after startup."""
    restored = 0
    db = SessionLocal()
    try:
        for task in load_queued_tasks(db, limit=limit):
            norm = os.path.normpath(task.path)
            if not os.path.isfile(norm):
                finish_task_by_id(task.id, "failed", "源文件不存在")
                continue
            with watcher._pending_lock:
                if norm in watcher._processed or norm in watcher._pending:
                    continue
                watcher._pending[norm] = time.time()
                getattr(watcher, "_pending_task_ids", {})[norm] = task.id
                getattr(watcher, "_pending_task_types", {})[norm] = task.task_type
                restored += 1
        return restored
    finally:
        db.close()
