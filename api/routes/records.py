"""Scrape record queries, manual match, retry, delete."""

import json
import logging
import os
import re
import shutil
import threading
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import or_
from typing import Optional

from core.models.media_item import MediaItem
from core.services.worker_context import WorkerContext
from core.workers.task_runner import process_task as _process_task
from db.database import get_db, vacuum_db
from db.scrape_models import ScrapeRecord, MonitorFolder
from db.tmdb_api import (
    fetch_bgm_candidates,
    fetch_tmdb_candidates,
    fetch_tmdb_by_id,
    fetch_bgm_by_id,
)
from utils.helpers import (
    candidate_to_result,
    invalidate_cache_prefix,
    DEFAULT_VIDEO_EXTS,
    DEFAULT_SUB_AUDIO_EXTS,
    normalize_parse_source,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/records", tags=["records"])


def _apply_parse_source_filter(q, parse_source: Optional[str]):
    if not parse_source:
        return q
    parse_source = str(parse_source).strip().lower()
    patterns = ("ai", "hybrid") if parse_source == "ai" else (parse_source,)
    from sqlalchemy import or_

    clauses = []
    for value in patterns:
        clauses.append(ScrapeRecord.metadata_json.like(f'%\"parse_source\": \"{value}\"%'))
        clauses.append(ScrapeRecord.metadata_json.like(f'%\"parse_source\":\"{value}\"%'))
    return q.filter(or_(*clauses))


class RecordOut(BaseModel):
    id: int
    folder_id: Optional[int] = None
    original_path: str
    original_name: str
    status: str
    matched_title: Optional[str] = None
    matched_id: Optional[str] = None
    matched_provider: Optional[str] = None
    target_path: Optional[str] = None
    media_type: Optional[str] = None
    parse_source: Optional[str] = None
    error_msg: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class ManualMatchBody(BaseModel):
    candidate_id: str
    candidate_title: str
    provider: str  # tmdb / bgm
    is_tv: bool = True
    season_override: Optional[int] = None
    episode_offset: int = 0
    scope: str = "single"  # "single" | "folder"


class SearchCandidatesBody(BaseModel):
    query: str
    year: Optional[int] = None
    is_tv: bool = True
    source: str = "siliconflow_tmdb"


def _row_to_out(r: ScrapeRecord) -> RecordOut:
    # Extract media_type and parse_source from stored metadata_json
    _media_type = None
    _parse_source = None
    if r.metadata_json:
        try:
            _meta = json.loads(r.metadata_json)
            _media_type = _meta.get("type")  # "episode" or "movie"
            _parse_source = normalize_parse_source(_meta.get("parse_source"))
        except Exception:
            pass
    return RecordOut(
        id=r.id,
        folder_id=r.folder_id,
        original_path=r.original_path,
        original_name=r.original_name,
        status=r.status,
        matched_title=r.matched_title,
        matched_id=r.matched_id,
        matched_provider=r.matched_provider,
        target_path=r.target_path,
        media_type=_media_type,
        parse_source=_parse_source,
        error_msg=r.error_msg,
        created_at=r.created_at.isoformat() if r.created_at else None,
        updated_at=r.updated_at.isoformat() if r.updated_at else None,
    )


@router.get("", response_model=dict)
def list_records(
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    parse_source: Optional[str] = None,
    dir: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    q = db.query(ScrapeRecord)
    if status:
        q = q.filter(ScrapeRecord.status == status)
    if keyword:
        q = q.filter(ScrapeRecord.original_name.ilike(f"%{keyword}%"))
    if media_type:
        q = q.join(MonitorFolder, ScrapeRecord.folder_id == MonitorFolder.id, isouter=True)
        q = q.filter(MonitorFolder.media_type == media_type)
    q = _apply_parse_source_filter(q, parse_source)
    if dir:
        # Filter records whose original_path is directly inside the given directory
        norm_dir = os.path.normpath(dir)
        # 用 SQL LIKE 过滤，避免全表加载到内存
        q = q.filter(
            or_(
                ScrapeRecord.target_path.like(norm_dir.replace('\\', '/') + '/%'),
                ScrapeRecord.target_path.like(norm_dir + os.sep + '%'),
                ScrapeRecord.original_path.like(norm_dir.replace('\\', '/') + '/%'),
                ScrapeRecord.original_path.like(norm_dir + os.sep + '%'),
            )
        )
    total = q.count()
    rows = q.order_by(ScrapeRecord.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_row_to_out(r).model_dump() for r in rows],
    }


@router.get("/grouped")
def list_records_grouped(
    status: Optional[str] = None,
    keyword: Optional[str] = None,
    media_type: Optional[str] = None,
    parse_source: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return records grouped by output directory when available."""
    q = db.query(ScrapeRecord)
    if status:
        q = q.filter(ScrapeRecord.status == status)
    if keyword:
        q = q.filter(ScrapeRecord.original_name.ilike(f"%{keyword}%"))
    if media_type:
        q = q.join(MonitorFolder, ScrapeRecord.folder_id == MonitorFolder.id, isouter=True)
        q = q.filter(MonitorFolder.media_type == media_type)
    q = _apply_parse_source_filter(q, parse_source)
    rows = q.order_by(ScrapeRecord.id.desc()).all()

    groups: dict = {}
    for r in rows:
        dir_path = _record_output_group_dir(r)
        if dir_path not in groups:
            groups[dir_path] = {
                "dir_path": dir_path,
                "dir_name": os.path.basename(dir_path),
                "folder_id": r.folder_id,
                "total": 0,
                "success": 0,
                "failed": 0,
                "pending": 0,
                "ids": [],
            }
        g = groups[dir_path]
        g["total"] += 1
        g["ids"].append(r.id)
        if r.status == "success":
            g["success"] += 1
        elif r.status == "failed":
            g["failed"] += 1
        elif r.status == "pending_manual":
            g["pending"] += 1

    return {"groups": list(groups.values())}


@router.delete("/{record_id}")
def delete_record(
    record_id: int,
    delete_files: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)
    files_deleted = 0
    if delete_files:
        files_deleted = _delete_record_files(row, db)
    db.delete(row)
    db.commit()
    return {"ok": True, "files_deleted": files_deleted}


class BatchDeleteBody(BaseModel):
    ids: list[int]
    delete_files: bool = False


class GroupDeleteBody(BaseModel):
    ids: list[int]
    group_dir: str


def _normcase_path(path: str) -> str:
    return os.path.normcase(os.path.normpath(str(path or "")))


def _record_output_group_dir(row: ScrapeRecord) -> str:
    target = str(row.target_path or "").strip()
    if target:
        return os.path.normpath(os.path.dirname(target))
    return os.path.normpath(os.path.dirname(row.original_path))


def _record_output_cleanup_paths(row: ScrapeRecord) -> list[str]:
    target = str(row.target_path or "").strip()
    if not target:
        return []

    paths = [target]
    stem, _ = os.path.splitext(target)
    for suffix in (".nfo", "-thumb.jpg"):
        paths.append(stem + suffix)
    return paths


def _directory_has_media(directory: str) -> bool:
    if not os.path.isdir(directory):
        return False
    for dirpath, _, filenames in os.walk(directory):
        for fn in filenames:
            if fn.lower().endswith(_MEDIA_EXTS):
                return True
    return False


def _delete_dir_sidecars_only(directory: str) -> int:
    if not os.path.isdir(directory):
        return 0

    deleted = 0
    try:
        names = os.listdir(directory)
    except Exception as err:
        logger.warning("Failed to list group directory %s: %s", directory, err)
        return 0

    for fn in names:
        fp = os.path.join(directory, fn)
        if not os.path.isfile(fp):
            continue
        fn_lower = fn.lower()
        should_remove = fn_lower in _DIR_SIDECAR_EXACT
        if not should_remove:
            for prefix, suffixes in _DIR_SIDECAR_PATTERNS:
                if fn_lower.startswith(prefix) and fn_lower.endswith(suffixes):
                    should_remove = True
                    break
        if not should_remove:
            continue
        if _safe_remove_file(fp):
            deleted += 1
    return deleted


def _safe_remove_file(path: str) -> bool:
    try:
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
            return True
    except Exception as err:
        logger.warning("Failed to remove file %s: %s", path, err)
    return False


def _delete_record_files(row: ScrapeRecord, db: Session, cleanup_dirs: bool = True) -> int:
    """Delete local files associated with a single record (target + sidecars).

    Works for all organize_modes:
    - move/rename/copy/hardlink: removes target_path and its sidecar files,
      then cleans up empty directories.
    - symlink: removes the symlink at target_path and its sidecar files.
    - If target_path is absent or gone, does nothing (safe to call regardless).

    When cleanup_dirs=False the caller is responsible for directory cleanup
    (use this in batch operations so dirs are cleaned once after all files
    are deleted, rather than after each individual file).

    Returns the number of files actually removed from disk.
    """
    target = str(row.target_path or "").strip()
    if not target:
        return 0

    deleted = 0
    paths = _record_output_cleanup_paths(row)
    for p in paths:
        if _safe_remove_file(p):
            deleted += 1

    # Clean up empty directories up to the target_root / monitor folder root
    if cleanup_dirs and target and deleted:
        folder = db.query(MonitorFolder).get(row.folder_id) if row.folder_id else None
        watch_root = None
        if folder:
            organize_mode = getattr(folder, "organize_mode", "move") or "move"
            watch_root = os.path.normpath(
                folder.path if organize_mode == "rename" else (folder.target_root or folder.path)
            )
        try:
            from monitor.watcher import _remove_empty_dirs
            _remove_empty_dirs(os.path.dirname(target), stop_at=watch_root)
        except Exception as e:
            logger.debug("Failed to clean empty dirs after delete: %s", e)

    return deleted


_IGNORABLE_NAMES = frozenset({
    "desktop.ini", "thumbs.db", ".ds_store", "picasa.ini",
    ".picasa.ini", "folder.jpg", ".bridgesort",
})


def _dir_has_real_content(dir_path: str) -> bool:
    """Return True if the directory contains files other than ignorable system metadata."""
    try:
        return any(n.lower() not in _IGNORABLE_NAMES for n in os.listdir(dir_path))
    except Exception:
        return True  # treat as non-empty on error


def _cleanup_empty_group_dir(group_dir: str) -> bool:
    """Remove *group_dir* if it contains no real files (ignores system metadata files).

    If only ignorable system files (desktop.ini, Thumbs.db, …) remain they are
    deleted first so that the directory can then be removed with os.rmdir.
    Returns True when the directory was successfully deleted.
    """
    try:
        if not os.path.isdir(group_dir):
            return False
        if _dir_has_real_content(group_dir):
            return False
        # Remove any lingering system metadata files
        for name in os.listdir(group_dir):
            try:
                os.remove(os.path.join(group_dir, name))
            except Exception:
                pass
        os.rmdir(group_dir)
        return True
    except Exception as err:
        logger.warning("Failed to remove group directory %s: %s", group_dir, err)
        return False


def _extract_group_season_number(group_dir: str, rows: list[ScrapeRecord]) -> Optional[int]:
    for row in rows:
        if not row.metadata_json:
            continue
        try:
            meta = json.loads(row.metadata_json)
        except Exception:
            continue
        value = meta.get("s")
        try:
            season_num = int(value)
        except Exception:
            continue
        if season_num >= 0:
            return season_num

    base = os.path.basename(os.path.normpath(group_dir))
    match = re.match(r"^(?:Season\s*|S)(\d+)$", base, re.I)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _season_root_sidecar_paths(group_dir: str, season_num: Optional[int]) -> list[str]:
    if season_num is None:
        return []
    show_root = os.path.dirname(os.path.normpath(group_dir))
    if not show_root or show_root == group_dir:
        return []
    season_fmt = f"{int(season_num):02d}"
    return [
        os.path.join(show_root, f"season{season_fmt}.nfo"),
        os.path.join(show_root, f"season{season_fmt}-poster.jpg"),
    ]


def _resolve_group_cleanup_root(rows: list[ScrapeRecord], folders_by_id: dict[int, MonitorFolder]) -> Optional[str]:
    roots = set()
    for row in rows:
        folder = folders_by_id.get(row.folder_id) if row.folder_id else None
        if not folder:
            continue
        organize_mode = getattr(folder, "organize_mode", "move") or "move"
        candidate = folder.path if organize_mode == "rename" else folder.target_root
        candidate = os.path.normpath(str(candidate or "").strip())
        if candidate:
            roots.add(os.path.normcase(candidate))
    if len(roots) != 1:
        return None

    only_root = next(iter(roots))
    for row in rows:
        folder = folders_by_id.get(row.folder_id) if row.folder_id else None
        if not folder:
            continue
        organize_mode = getattr(folder, "organize_mode", "move") or "move"
        candidate = folder.path if organize_mode == "rename" else folder.target_root
        candidate = os.path.normpath(str(candidate or "").strip())
        if os.path.normcase(candidate) == only_root:
            return candidate
    return None


def _cleanup_parent_show_dir(
    group_dir: str,
    season_num: Optional[int],
    cleanup_root: Optional[str],
) -> tuple[int, bool]:
    show_root = os.path.dirname(os.path.normpath(group_dir))
    if not show_root or show_root == group_dir:
        return 0, False

    files_deleted = 0
    if not _directory_has_media(group_dir):
        for path in _season_root_sidecar_paths(group_dir, season_num):
            if _safe_remove_file(path):
                files_deleted += 1

    if _directory_has_media(show_root):
        return files_deleted, False

    files_deleted += _delete_dir_sidecars_only(show_root)

    dir_deleted = False
    if cleanup_root:
        from monitor.watcher import _remove_empty_dirs

        before_exists = os.path.isdir(show_root)
        _remove_empty_dirs(show_root, stop_at=cleanup_root)
        dir_deleted = before_exists and not os.path.exists(show_root)
    else:
        dir_deleted = _cleanup_empty_group_dir(show_root)

    return files_deleted, dir_deleted


@router.post("/batch-delete")
def batch_delete(body: BatchDeleteBody, db: Session = Depends(get_db)):
    """Delete multiple records by IDs, optionally also deleting local files."""
    files_deleted = 0
    if body.delete_files:
        rows = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(body.ids)).all()

        # Collect (target_dir, watch_root) pairs before deleting files
        dir_roots: list[tuple[str, Optional[str]]] = []
        for row in rows:
            target = str(row.target_path or "").strip()
            if not target:
                continue
            folder = db.query(MonitorFolder).get(row.folder_id) if row.folder_id else None
            watch_root = None
            if folder:
                organize_mode = getattr(folder, "organize_mode", "move") or "move"
                watch_root = os.path.normpath(
                    folder.path if organize_mode == "rename" else (folder.target_root or folder.path)
                )
            dir_roots.append((os.path.dirname(target), watch_root))

        # Delete all files first (cleanup_dirs=False, do it once after all deletions)
        for row in rows:
            files_deleted += _delete_record_files(row, db, cleanup_dirs=False)

        # Now clean up empty directories once, after all files are gone
        try:
            from monitor.watcher import _remove_empty_dirs
            seen_dirs: set[str] = set()
            for dir_path, watch_root in dir_roots:
                norm = os.path.normcase(dir_path)
                if norm in seen_dirs:
                    continue
                seen_dirs.add(norm)
                _remove_empty_dirs(dir_path, stop_at=watch_root)
        except Exception as e:
            logger.debug("Failed to clean empty dirs after batch delete: %s", e)

    deleted = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(body.ids)).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted, "files_deleted": files_deleted}


@router.post("/delete-group")
def delete_group(body: GroupDeleteBody, db: Session = Depends(get_db)):
    group_dir = os.path.normpath(str(body.group_dir or "").strip())
    if not group_dir:
        raise HTTPException(400, detail="分组目录不能为空")
    if not body.ids:
        return {
            "ok": True,
            "deleted": 0,
            "files_deleted": 0,
            "dir_deleted": False,
            "group_dir": group_dir,
        }

    rows = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(body.ids)).all()
    if not rows:
        return {
            "ok": True,
            "deleted": 0,
            "files_deleted": 0,
            "dir_deleted": False,
            "group_dir": group_dir,
        }

    folder_ids = {row.folder_id for row in rows if row.folder_id}
    folders_by_id = {}
    if folder_ids:
        folders = db.query(MonitorFolder).filter(MonitorFolder.id.in_(folder_ids)).all()
        folders_by_id = {folder.id: folder for folder in folders}
    cleanup_root = _resolve_group_cleanup_root(rows, folders_by_id)
    season_num = _extract_group_season_number(group_dir, rows)

    expected_group = _normcase_path(group_dir)
    files_to_delete = []
    for row in rows:
        row_group = _normcase_path(_record_output_group_dir(row))
        if row_group != expected_group:
            raise HTTPException(400, detail="分组记录与目标目录不匹配，已取消删除")
        files_to_delete.extend(_record_output_cleanup_paths(row))

    other_rows = db.query(ScrapeRecord).filter(~ScrapeRecord.id.in_(body.ids)).all()
    blocked_files = set()
    for row in other_rows:
        row_group = _normcase_path(_record_output_group_dir(row))
        if row_group != expected_group:
            continue
        for path in _record_output_cleanup_paths(row):
            blocked_files.add(_normcase_path(path))

    can_touch_group_dir = bool(files_to_delete)
    files_deleted = 0
    visited = set()
    for path in files_to_delete:
        norm_path = _normcase_path(path)
        if not norm_path or norm_path in visited or norm_path in blocked_files:
            continue
        visited.add(norm_path)
        if _safe_remove_file(path):
            files_deleted += 1

    if can_touch_group_dir:
        files_deleted += _delete_dir_sidecars_only(group_dir)

    deleted = (
        db.query(ScrapeRecord)
        .filter(ScrapeRecord.id.in_(body.ids))
        .delete(synchronize_session=False)
    )
    db.commit()
    dir_deleted = _cleanup_empty_group_dir(group_dir) if can_touch_group_dir else False
    parent_files_deleted = 0
    parent_dir_deleted = False
    if can_touch_group_dir:
        parent_files_deleted, parent_dir_deleted = _cleanup_parent_show_dir(
            group_dir,
            season_num,
            cleanup_root,
        )
        files_deleted += parent_files_deleted
    return {
        "ok": True,
        "deleted": deleted,
        "files_deleted": files_deleted,
        "dir_deleted": dir_deleted or parent_dir_deleted,
        "group_dir": group_dir,
    }


@router.post("/clear-failed")
def clear_failed(db: Session = Depends(get_db)):
    """Delete all failed records."""
    deleted = db.query(ScrapeRecord).filter(ScrapeRecord.status == "failed").delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


@router.post("/clear-all")
def clear_all(db: Session = Depends(get_db)):
    """Delete all records."""
    deleted = db.query(ScrapeRecord).delete(synchronize_session=False)
    db.commit()
    db.close()
    vacuum_db()
    return {"ok": True, "deleted": deleted}


@router.post("/batch-retry")
def batch_retry(body: BatchDeleteBody, db: Session = Depends(get_db)):
    """Retry multiple records by deleting them and re-enqueuing the original files."""
    rows = db.query(ScrapeRecord).filter(
        ScrapeRecord.id.in_(body.ids),
    ).all()

    # Collect paths of files that still exist on disk before deleting records
    paths_to_retry = [
        row.original_path for row in rows
        if os.path.isfile(row.original_path)
    ]

    # Delete the old records so _process_file can re-create them fresh
    for row in rows:
        db.delete(row)
    db.commit()

    # Re-enqueue via watcher
    from server import get_watcher
    w = get_watcher()
    count = 0
    if w:
        # Group paths by directory to enable cache reuse
        from collections import defaultdict
        dir_groups = defaultdict(list)
        for path in paths_to_retry:
            dir_groups[os.path.dirname(path)].append(path)

        # Process files directory by directory, sequentially within each directory
        for dir_path, paths in dir_groups.items():
            for path in sorted(paths):  # Sort to ensure consistent order
                norm = os.path.normpath(path)
                with w._pending_lock:
                    w._processed.discard(norm)
                # Submit to pool but files in same directory will be processed sequentially
                # because we wait for each directory group to complete before moving to next
                w._pool.submit(w._process_file, path)
                with w._pending_lock:
                    w._processed.add(norm)
                count += 1
                # Small delay to ensure first file writes cache before next file reads it
                import time
                time.sleep(0.1)

    return {"ok": True, "count": count}


@router.post("/search-candidates")
def search_candidates(body: SearchCandidatesBody, db: Session = Depends(get_db)):
    """Search TMDB/BGM candidates for manual matching."""
    from utils.helpers import CONFIG_FILE
    ctx = WorkerContext()
    api_key = ctx.tmdb_api_key.get() if body.source == "siliconflow_tmdb" else ctx.bgm_api_key.get()

    if body.source == "siliconflow_tmdb":
        results = fetch_tmdb_candidates(body.query, body.year, body.is_tv, api_key)
    else:
        results = fetch_bgm_candidates(body.query, body.year, api_key)

    return {"candidates": results or []}


# ------------------------------------------------------------------
# Helpers: restore a record to pre-archive state
# ------------------------------------------------------------------

def _delete_file_sidecars(file_path: str):
    """Delete per-file sidecar files (NFO + thumbnail) for a given media file."""
    stem = os.path.splitext(file_path)[0]
    for suffix in (".nfo", "-thumb.jpg"):
        p = stem + suffix
        if os.path.isfile(p):
            try:
                os.remove(p)
                logger.debug(f"Deleted sidecar: {p}")
            except Exception as e:
                logger.warning(f"Failed to delete sidecar {p}: {e}")


_MEDIA_EXTS = tuple(
    e.strip()
    for e in (DEFAULT_VIDEO_EXTS + "," + DEFAULT_SUB_AUDIO_EXTS).split(",")
    if e.strip()
)
# 目录级 sidecar 文件名（完整匹配）
_DIR_SIDECAR_EXACT = {
    "tvshow.nfo", "poster.jpg", "fanart.jpg", "season.nfo", "folder.jpg",
}
# 目录级 sidecar 前缀+后缀匹配（如 season01.nfo, season01-poster.jpg）
_DIR_SIDECAR_PATTERNS = (
    ("season", (".nfo", "-poster.jpg")),
)


def _cleanup_dir_sidecars(target_file: str, watch_root: Optional[str] = None):
    """文件移走后，若目录（及其父目录）中不再含有媒体文件，则清理目录级 sidecar。

    处理两层目录：
      - target_dir（如 Season 1/）— season.nfo, folder.jpg, seasonXX-poster.jpg …
      - target_parent（如 剧名{tmdbid-X}/）— tvshow.nfo, poster.jpg, fanart.jpg …
    """

    def _has_media(directory: str) -> bool:
        for dirpath, _, filenames in os.walk(directory):
            for fn in filenames:
                if fn.lower().endswith(_MEDIA_EXTS):
                    return True
        return False

    def _delete_dir_level_sidecars(directory: str):
        try:
            for fn in os.listdir(directory):
                fp = os.path.join(directory, fn)
                if not os.path.isfile(fp):
                    continue
                fn_lower = fn.lower()
                if fn_lower in _DIR_SIDECAR_EXACT:
                    _safe_remove(fp)
                else:
                    for prefix, suffixes in _DIR_SIDECAR_PATTERNS:
                        if fn_lower.startswith(prefix) and fn_lower.endswith(suffixes):
                            _safe_remove(fp)
                            break
        except Exception as e:
            logger.warning(f"Failed to list dir for sidecar cleanup {directory}: {e}")

    def _safe_remove(fp: str):
        try:
            os.remove(fp)
            logger.debug(f"Deleted dir sidecar: {fp}")
        except Exception as e:
            logger.warning(f"Failed to delete dir sidecar {fp}: {e}")

    tgt_dir = os.path.normpath(os.path.dirname(target_file))
    tgt_parent = os.path.normpath(os.path.dirname(tgt_dir))

    if not _has_media(tgt_dir):
        _delete_dir_level_sidecars(tgt_dir)

    if tgt_parent != tgt_dir and (
        watch_root is None
        or os.path.normcase(tgt_parent) != os.path.normcase(watch_root)
    ):
        if not _has_media(tgt_parent):
            _delete_dir_level_sidecars(tgt_parent)


def _restore_record_file(row: ScrapeRecord, folder, db: Session):
    """Restore an already-archived record back to its original state.

    - Deletes per-file sidecar files at target_path
    - Moves / removes the target file depending on organize_mode
    - Validates that original_path exists afterwards
    - Resets row fields (does NOT commit — caller must commit)
    """
    tgt = row.target_path
    if not tgt or not os.path.exists(tgt):
        # Nothing archived yet, or file gone — just validate original
        if not os.path.isfile(row.original_path):
            raise HTTPException(400, detail="源文件不存在，无法恢复")
        return

    organize_mode = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'

    # 1. Delete per-file sidecar files at target location
    _delete_file_sidecars(tgt)

    # 2. Restore / remove the target file
    if os.path.normcase(os.path.normpath(tgt)) != os.path.normcase(os.path.normpath(row.original_path)):
        from monitor.watcher import _remove_empty_dirs
        watch_root = os.path.normpath(folder.path) if folder else None
        if organize_mode in ('move', 'rename'):
            # File was moved — move it back
            orig_dir = os.path.dirname(row.original_path)
            os.makedirs(orig_dir, exist_ok=True)
            shutil.move(tgt, row.original_path)
            logger.info(f"Restored: {tgt} -> {row.original_path}")
            # Clean up directory-level sidecars if no media files remain
            _cleanup_dir_sidecars(tgt, watch_root=watch_root)
            # Clean up empty directories left behind
            _remove_empty_dirs(os.path.dirname(tgt), stop_at=watch_root)
        else:
            # copy / symlink / hardlink — original is still in place, just remove target
            try:
                os.remove(tgt)
                logger.info(f"Removed target copy/link: {tgt}")
                # Clean up directory-level sidecars if no media files remain
                _cleanup_dir_sidecars(tgt, watch_root=watch_root)
                _remove_empty_dirs(os.path.dirname(tgt), stop_at=watch_root)
            except Exception as e:
                logger.warning(f"Failed to remove target {tgt}: {e}")

    if not os.path.isfile(row.original_path):
        raise HTTPException(400, detail="源文件恢复失败，文件不存在")

    # 3. Reset record fields
    row.target_path = None
    row.status = "processing"
    row.error_msg = None
    row.matched_title = None
    row.matched_id = None
    row.matched_provider = None
    row.metadata_json = None
    db.flush()


def _archive_file(item, row, folder, ctx, tid, provider, db):
    """Archive a single file after successful process_task, respecting organize_mode."""
    organize_mode = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'

    # For 'rename' mode, use the monitored folder path as target_root
    if organize_mode == 'rename' and folder:
        ctx.target_root.set(folder.path)

    target = item.full_target or os.path.join(item.dir, item.new_name_only or item.old_name)
    target_dir = os.path.dirname(target)
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    if os.path.normcase(item.path) != os.path.normcase(target):
        # Check if source and target are actually the same file on disk
        _same_file = False
        if os.path.exists(target) and os.path.isfile(item.path):
            try:
                _same_file = os.path.samefile(item.path, target)
            except (OSError, ValueError):
                pass

        if _same_file:
            # File is already in the right place — skip move, just scrape
            item.path = target
        elif os.path.exists(target):
            if not os.path.isfile(item.path):
                # 目标已存在且源文件已消失，说明上次整理已完成，
                # 直接更新 sidecar 和数据库状态，不视为失败。
                ctx._write_sidecar_files(item, target)
                row.status = "success"
                row.matched_title = (item.metadata or {}).get("title")
                row.matched_id = str(tid)
                row.matched_provider = provider
                row.target_path = target
                row.metadata_json = json.dumps(item.metadata or {}, ensure_ascii=False)
                row.error_msg = None
                db.flush()
                return target
            row.status = "failed"
            row.target_path = target
            row.error_msg = f"目标文件已存在: {target}"
            db.commit()
            raise HTTPException(400, detail=f"目标文件已存在: {target}")
        else:
            src_dir = os.path.dirname(item.path)

            if organize_mode == 'copy':
                shutil.copy2(item.path, target)
            elif organize_mode == 'symlink':
                os.symlink(os.path.abspath(item.path), target)
            elif organize_mode == 'hardlink':
                os.link(item.path, target)
            else:
                # move / rename — both use shutil.move
                shutil.move(item.path, target)

            # For modes that keep the source file, don't clean up source dirs
            if organize_mode not in ('copy', 'symlink', 'hardlink'):
                item.path = target
                watch_root = os.path.normpath(folder.path) if folder else None
                from monitor.watcher import _remove_empty_dirs
                _remove_empty_dirs(src_dir, stop_at=watch_root)
            else:
                item.path = target

    ctx._write_sidecar_files(item, target)

    row.status = "success"
    row.matched_title = (item.metadata or {}).get("title")
    row.matched_id = str(tid)
    row.matched_provider = provider
    row.target_path = target
    row.metadata_json = json.dumps(item.metadata or {}, ensure_ascii=False)
    row.error_msg = None
    db.flush()
    return target


def _process_single_manual(row, body, folder, db):
    """Process a single record through manual match: restore → recognize → archive.
    Returns the updated row on success, raises HTTPException on failure.
    """
    # Restore if already archived
    if row.target_path and os.path.exists(row.target_path):
        _restore_record_file(row, folder, db)
        db.commit()

    if not os.path.isfile(row.original_path):
        row.status = "failed"
        row.error_msg = "源文件不存在"
        db.commit()
        raise HTTPException(400, detail="源文件不存在")

    # 手动识别时自动清除该作品所有集数缓存，确保使用最新数据而非错误的旧缓存
    if body.provider == "tmdb" and body.is_tv:
        invalidate_cache_prefix(f"tmdb_ep_v3:{body.candidate_id}_")

    # Fetch full metadata by ID
    ctx = WorkerContext()
    if body.provider == "tmdb":
        t, tid, msg, meta = fetch_tmdb_by_id(body.candidate_id, body.is_tv, ctx.tmdb_api_key.get())
    else:
        t, tid, msg, meta = fetch_bgm_by_id(body.candidate_id, ctx.bgm_api_key.get())

    if tid == "None":
        raise HTTPException(400, detail="候选 ID 无效")

    item = MediaItem(
        id=str(uuid.uuid4()),
        path=row.original_path,
        dir=os.path.dirname(row.original_path),
        old_name=row.original_name,
        ext=os.path.splitext(row.original_name)[1],
    )

    # Apply folder settings
    organize_mode = getattr(folder, 'organize_mode', 'move') or 'move' if folder else 'move'
    if organize_mode == 'rename' and folder:
        # rename 模式：以监控目录本身为 target_root，process_task 才能生成正确的 full_target
        # （含 [tmdbid=xxx] 的父文件夹名），不能等到 _archive_file 里才设置
        ctx.target_root.set(folder.path)
    elif folder and folder.target_root:
        ctx.target_root.set(folder.target_root)
    if folder and folder.data_source:
        ctx.source_var.set(folder.data_source)

    ctx.media_type_override.set("电视剧" if body.is_tv else "电影")

    ctx.manual_locks[item.path] = (body.candidate_title, str(body.candidate_id), f"手动/{body.provider}命中", meta or {})
    if body.season_override is not None:
        ctx.forced_seasons[item.path] = body.season_override
    if body.episode_offset != 0:
        ctx.forced_offsets[item.path] = body.episode_offset
    ctx.file_list = [item]

    try:
        _process_task(ctx, 0)
    except Exception as e:
        logger.error(f"Manual match process_task failed: {e}")
        row.status = "failed"
        row.error_msg = str(e)[:500]
        db.commit()
        raise HTTPException(500, detail=f"识别失败: {str(e)[:100]}")

    try:
        _archive_file(item, row, folder, ctx, tid, body.provider, db)
        db.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Manual match archive failed: {e}")
        row.status = "failed"
        row.error_msg = str(e)[:500]
        db.commit()
        raise HTTPException(500, detail=f"归档失败: {str(e)[:100]}")

    # Broadcast
    from server import get_watcher
    w = get_watcher()
    if w and w._broadcast:
        from monitor.watcher import _record_to_dict
        w._broadcast({"type": "record_update", "data": _record_to_dict(row)})

    # TG notification
    if w and hasattr(w, '_tg_batcher') and folder:
        w._tg_batcher.add(folder.id, os.path.basename(folder.path), item)

    return row


@router.post("/{record_id}/manual-match")
def manual_match(record_id: int, body: ManualMatchBody, db: Session = Depends(get_db)):
    """Apply a manually chosen candidate to a pending record, then archive."""
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)

    folder = db.query(MonitorFolder).get(row.folder_id) if row.folder_id else None

    # Process the primary record
    _process_single_manual(row, body, folder, db)

    processed_count = 1

    # Folder scope: also process sibling files in the same directory
    if body.scope == "folder":
        org_dir = os.path.normpath(os.path.dirname(row.original_path))
        siblings = db.query(ScrapeRecord).filter(
            ScrapeRecord.folder_id == row.folder_id,
            ScrapeRecord.id != row.id,
        ).all()
        siblings = [s for s in siblings
                    if os.path.normpath(os.path.dirname(s.original_path)) == org_dir]

        for sib in siblings:
            try:
                _process_single_manual(sib, body, folder, db)
                processed_count += 1
            except Exception as e:
                logger.error(f"Folder-scope manual match failed for {sib.original_path}: {e}")

    return {"ok": True, "processed": processed_count}


@router.post("/{record_id}/retry")
def retry_record(record_id: int, db: Session = Depends(get_db)):
    """Re-run automatic recognition on a failed/pending record."""
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)
    if not os.path.isfile(row.original_path):
        row.status = "failed"
        row.error_msg = "源文件不存在"
        db.commit()
        raise HTTPException(400, detail="源文件不存在")

    row.status = "processing"
    row.error_msg = None
    db.commit()

    from server import get_watcher
    w = get_watcher()

    def _run():
        if w:
            w._process_file(row.original_path)

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "message": "重试已启动"}


# ------------------------------------------------------------------
# Metadata refresh — re-fetch from TMDB/BGM without full re-recognition
# ------------------------------------------------------------------

@router.post("/{record_id}/refresh-metadata")
def refresh_metadata(record_id: int, db: Session = Depends(get_db)):
    """Re-fetch metadata for a successful record and update NFO + images."""
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)
    if row.status != "success":
        raise HTTPException(400, detail="只能刷新已成功的记录")
    if not row.matched_id or not row.metadata_json:
        raise HTTPException(400, detail="记录缺少匹配信息")

    from server import get_watcher
    from monitor.watcher import refresh_record_metadata, _record_to_dict

    w = get_watcher()
    worker_ctx = w._worker_ctx if w else WorkerContext()
    broadcast_fn = w._broadcast if w else (lambda d: None)

    try:
        updated = refresh_record_metadata(row, db, worker_ctx, broadcast_fn)
        if updated:
            broadcast_fn({"type": "record_update", "data": _record_to_dict(row)})
            return {"ok": True, "message": "元数据已刷新", "updated": True}
        return {"ok": True, "message": "元数据已是最新，无需刷新", "updated": False}
    except Exception as e:
        logger.error(f"Metadata refresh failed for record {record_id}: {e}")
        raise HTTPException(500, detail=f"刷新失败: {str(e)[:200]}")


class BatchRefreshBody(BaseModel):
    ids: list[int]


@router.post("/batch-refresh-metadata")
def batch_refresh_metadata(body: BatchRefreshBody, db: Session = Depends(get_db)):
    """Re-fetch metadata for multiple successful records."""
    rows = db.query(ScrapeRecord).filter(
        ScrapeRecord.id.in_(body.ids),
        ScrapeRecord.status == "success",
        ScrapeRecord.matched_id.isnot(None),
        ScrapeRecord.metadata_json.isnot(None),
    ).all()

    if not rows:
        return {"ok": True, "total": 0, "updated": 0, "message": "没有符合条件的记录"}

    from server import get_watcher
    from monitor.watcher import refresh_record_metadata, _record_to_dict

    w = get_watcher()
    worker_ctx = w._worker_ctx if w else WorkerContext()
    broadcast_fn = w._broadcast if w else (lambda d: None)

    updated_count = 0
    for row in rows:
        try:
            if refresh_record_metadata(row, db, worker_ctx, broadcast_fn):
                updated_count += 1
                broadcast_fn({"type": "record_update", "data": _record_to_dict(row)})
        except Exception as e:
            logger.warning(f"Batch metadata refresh failed for record {row.id}: {e}")
        import time
        time.sleep(1.0)  # Rate-limit friendly

    return {
        "ok": True,
        "total": len(rows),
        "updated": updated_count,
        "message": f"已刷新 {updated_count}/{len(rows)} 条记录",
    }
