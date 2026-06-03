import os
import threading
from typing import Iterable

from fastapi import HTTPException
from sqlalchemy.orm import Session

from db.database import vacuum_db
from db.scrape_models import SymlinkRecord


def _normcase_path(path: str) -> str:
    return os.path.normcase(os.path.normpath(str(path or "")))


def _symlink_group_dir(row: SymlinkRecord) -> str:
    link_path = str(row.link_path or "").strip()
    if link_path:
        return os.path.normpath(os.path.dirname(link_path))
    return os.path.normpath(os.path.dirname(row.original_path))


def _safe_remove_path(path: str) -> bool:
    try:
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
            return True
    except Exception:
        return False
    return False


def _cleanup_empty_group_dir(group_dir: str) -> bool:
    try:
        if not os.path.isdir(group_dir):
            return False
        if os.listdir(group_dir):
            return False
        os.rmdir(group_dir)
        return True
    except Exception:
        return False


def _remove_empty_dirs(dir_paths: Iterable[str]):
    try:
        from monitor.watcher import _remove_empty_dirs as remove_empty_dirs_impl

        seen_dirs: set[str] = set()
        for dir_path in dir_paths:
            norm = os.path.normcase(dir_path)
            if norm in seen_dirs:
                continue
            seen_dirs.add(norm)
            remove_empty_dirs_impl(dir_path, stop_at=None)
    except Exception:
        pass


def clear_all_symlinks(db: Session):
    deleted = db.query(SymlinkRecord).delete(synchronize_session=False)
    db.commit()
    db.close()
    vacuum_db()
    return {"ok": True, "deleted": deleted}


def delete_symlink_record(db: Session, record_id: int, delete_files: bool = False):
    row = db.query(SymlinkRecord).get(record_id)
    if not row:
        raise HTTPException(404, detail="记录不存在")
    files_deleted = 0
    if delete_files:
        link = str(row.link_path or "").strip()
        if link and _safe_remove_path(link):
            files_deleted += 1
            _remove_empty_dirs([os.path.dirname(link)])
    db.delete(row)
    db.commit()
    return {"ok": True, "files_deleted": files_deleted}


def batch_delete_symlink_records(db: Session, ids: list[int], delete_files: bool = False):
    if not ids:
        return {"ok": True, "deleted": 0, "files_deleted": 0}
    files_deleted = 0
    affected_dirs: list[str] = []
    if delete_files:
        rows = db.query(SymlinkRecord).filter(SymlinkRecord.id.in_(ids)).all()
        for row in rows:
            link = str(row.link_path or "").strip()
            if not link:
                continue
            affected_dirs.append(os.path.dirname(link))
            if _safe_remove_path(link):
                files_deleted += 1
        _remove_empty_dirs(affected_dirs)
    deleted = (
        db.query(SymlinkRecord)
        .filter(SymlinkRecord.id.in_(ids))
        .delete(synchronize_session=False)
    )
    db.commit()
    return {"ok": True, "deleted": deleted, "files_deleted": files_deleted}


def delete_symlink_group(db: Session, ids: list[int], group_dir: str):
    group_dir = os.path.normpath(str(group_dir or "").strip())
    if not group_dir:
        raise HTTPException(400, detail="分组目录不能为空")
    if not ids:
        return {
            "ok": True,
            "deleted": 0,
            "files_deleted": 0,
            "dir_deleted": False,
            "group_dir": group_dir,
        }

    rows = db.query(SymlinkRecord).filter(SymlinkRecord.id.in_(ids)).all()
    if not rows:
        return {
            "ok": True,
            "deleted": 0,
            "files_deleted": 0,
            "dir_deleted": False,
            "group_dir": group_dir,
        }

    expected_group = _normcase_path(group_dir)
    files_to_delete = []
    for row in rows:
        row_group = _normcase_path(_symlink_group_dir(row))
        if row_group != expected_group:
            raise HTTPException(400, detail="分组记录与目标目录不匹配，已取消删除")
        link_path = str(row.link_path or "").strip()
        if link_path:
            files_to_delete.append(link_path)

    other_rows = db.query(SymlinkRecord).filter(~SymlinkRecord.id.in_(ids)).all()
    blocked_files = set()
    for row in other_rows:
        row_group = _normcase_path(_symlink_group_dir(row))
        if row_group != expected_group:
            continue
        link_path = str(row.link_path or "").strip()
        if link_path:
            blocked_files.add(_normcase_path(link_path))

    can_touch_group_dir = bool(files_to_delete)
    files_deleted = 0
    visited = set()
    for path in files_to_delete:
        norm_path = _normcase_path(path)
        if not norm_path or norm_path in visited or norm_path in blocked_files:
            continue
        visited.add(norm_path)
        if _safe_remove_path(path):
            files_deleted += 1

    deleted = (
        db.query(SymlinkRecord)
        .filter(SymlinkRecord.id.in_(ids))
        .delete(synchronize_session=False)
    )
    db.commit()
    dir_deleted = _cleanup_empty_group_dir(group_dir) if can_touch_group_dir else False
    return {
        "ok": True,
        "deleted": deleted,
        "files_deleted": files_deleted,
        "dir_deleted": dir_deleted,
        "group_dir": group_dir,
    }


def clear_failed_symlinks(db: Session):
    deleted = (
        db.query(SymlinkRecord)
        .filter(SymlinkRecord.status == "failed")
        .delete(synchronize_session=False)
    )
    db.commit()
    return {"ok": True, "deleted": deleted}


def _queue_retry_paths(paths: list[str]):
    from server import get_watcher

    watcher = get_watcher()

    def _run():
        if watcher:
            for path in paths:
                watcher._process_file(path)

    threading.Thread(target=_run, daemon=True).start()


def retry_symlink_record(db: Session, record_id: int):
    row = db.query(SymlinkRecord).get(record_id)
    if not row:
        raise HTTPException(404, detail="记录不存在")
    if not os.path.isfile(row.original_path):
        raise HTTPException(400, detail="源文件不存在")
    path = row.original_path
    db.delete(row)
    db.commit()
    _queue_retry_paths([path])
    return {"ok": True}


def retry_all_failed_symlinks(db: Session):
    rows = db.query(SymlinkRecord).filter(SymlinkRecord.status == "failed").all()
    paths = [row.original_path for row in rows if os.path.isfile(row.original_path)]
    for row in rows:
        db.delete(row)
    db.commit()
    _queue_retry_paths(paths)
    return {"ok": True, "queued": len(paths)}
