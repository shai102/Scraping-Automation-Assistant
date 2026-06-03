"""Deletion and cleanup helpers for scrape records."""

import json
import logging
import os
import re
from typing import Optional

from fastapi import HTTPException

from db.scrape_models import MonitorFolder, ScrapeRecord
from monitor.delete_sync import remove_empty_dirs
from utils.media_defaults import DEFAULT_SUB_AUDIO_EXTS, DEFAULT_VIDEO_EXTS

logger = logging.getLogger(__name__)

MEDIA_EXTS = tuple(
    ext.strip()
    for ext in (DEFAULT_VIDEO_EXTS + "," + DEFAULT_SUB_AUDIO_EXTS).split(",")
    if ext.strip()
)

DIR_SIDECAR_EXACT = {
    "tvshow.nfo",
    "poster.jpg",
    "fanart.jpg",
    "season.nfo",
    "folder.jpg",
}

DIR_SIDECAR_PATTERNS = (
    ("season", (".nfo", "-poster.jpg")),
)

IGNORABLE_NAMES = frozenset(
    {
        "desktop.ini",
        "thumbs.db",
        ".ds_store",
        "picasa.ini",
        ".picasa.ini",
        "folder.jpg",
        ".bridgesort",
    }
)


def normcase_path(path: str) -> str:
    return os.path.normcase(os.path.normpath(str(path or "")))


def record_output_group_dir(row: ScrapeRecord) -> str:
    target = str(row.target_path or "").strip()
    if target:
        return os.path.normpath(os.path.dirname(target))
    return os.path.normpath(os.path.dirname(row.original_path))


def record_output_cleanup_paths(row: ScrapeRecord) -> list[str]:
    target = str(row.target_path or "").strip()
    if not target:
        return []

    paths = [target]
    stem, _ = os.path.splitext(target)
    for suffix in (".nfo", "-thumb.jpg"):
        paths.append(stem + suffix)
    return paths


def directory_has_media(directory: str) -> bool:
    if not os.path.isdir(directory):
        return False
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            if filename.lower().endswith(MEDIA_EXTS):
                return True
    return False


def safe_remove_file(path: str) -> bool:
    try:
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
            return True
    except Exception as err:
        logger.warning("Failed to remove file %s: %s", path, err)
    return False


def delete_dir_sidecars_only(directory: str) -> int:
    if not os.path.isdir(directory):
        return 0

    deleted = 0
    try:
        names = os.listdir(directory)
    except Exception as err:
        logger.warning("Failed to list group directory %s: %s", directory, err)
        return 0

    for filename in names:
        file_path = os.path.join(directory, filename)
        if not os.path.isfile(file_path):
            continue
        filename_lower = filename.lower()
        should_remove = filename_lower in DIR_SIDECAR_EXACT
        if not should_remove:
            for prefix, suffixes in DIR_SIDECAR_PATTERNS:
                if filename_lower.startswith(prefix) and filename_lower.endswith(suffixes):
                    should_remove = True
                    break
        if should_remove and safe_remove_file(file_path):
            deleted += 1
    return deleted


def delete_record_files(row: ScrapeRecord, db, cleanup_dirs: bool = True) -> int:
    """Delete target file + sidecars, then optionally prune empty directories."""
    target = str(row.target_path or "").strip()
    if not target:
        return 0

    deleted = 0
    for path in record_output_cleanup_paths(row):
        if safe_remove_file(path):
            deleted += 1

    if cleanup_dirs and target and deleted:
        folder = db.query(MonitorFolder).get(row.folder_id) if row.folder_id else None
        watch_root = None
        if folder:
            organize_mode = getattr(folder, "organize_mode", "move") or "move"
            watch_root = os.path.normpath(
                folder.path if organize_mode == "rename" else (folder.target_root or folder.path)
            )
        try:
            remove_empty_dirs(os.path.dirname(target), stop_at=watch_root)
        except Exception as err:
            logger.debug("Failed to clean empty dirs after delete: %s", err)

    return deleted


def dir_has_real_content(dir_path: str) -> bool:
    try:
        return any(name.lower() not in IGNORABLE_NAMES for name in os.listdir(dir_path))
    except Exception:
        return True


def cleanup_empty_group_dir(group_dir: str) -> bool:
    try:
        if not os.path.isdir(group_dir):
            return False
        if dir_has_real_content(group_dir):
            return False
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


def extract_group_season_number(group_dir: str, rows: list[ScrapeRecord]) -> Optional[int]:
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


def season_root_sidecar_paths(group_dir: str, season_num: Optional[int]) -> list[str]:
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


def resolve_group_cleanup_root(
    rows: list[ScrapeRecord],
    folders_by_id: dict[int, MonitorFolder],
) -> Optional[str]:
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


def cleanup_parent_show_dir(
    group_dir: str,
    season_num: Optional[int],
    cleanup_root: Optional[str],
) -> tuple[int, bool]:
    show_root = os.path.dirname(os.path.normpath(group_dir))
    if not show_root or show_root == group_dir:
        return 0, False

    files_deleted = 0
    if not directory_has_media(group_dir):
        for path in season_root_sidecar_paths(group_dir, season_num):
            if safe_remove_file(path):
                files_deleted += 1

    if directory_has_media(show_root):
        return files_deleted, False

    files_deleted += delete_dir_sidecars_only(show_root)

    dir_deleted = False
    if cleanup_root:
        before_exists = os.path.isdir(show_root)
        remove_empty_dirs(show_root, stop_at=cleanup_root)
        dir_deleted = before_exists and not os.path.exists(show_root)
    else:
        dir_deleted = cleanup_empty_group_dir(show_root)

    return files_deleted, dir_deleted


def delete_record_by_id(record_id: int, delete_files: bool, db) -> dict:
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)
    files_deleted = delete_record_files(row, db) if delete_files else 0
    db.delete(row)
    db.commit()
    return {"ok": True, "files_deleted": files_deleted}


def batch_delete_records(ids: list[int], delete_files: bool, db) -> dict:
    files_deleted = 0
    if delete_files:
        rows = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(ids)).all()
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

        for row in rows:
            files_deleted += delete_record_files(row, db, cleanup_dirs=False)

        seen_dirs: set[str] = set()
        for dir_path, watch_root in dir_roots:
            norm = os.path.normcase(dir_path)
            if norm in seen_dirs:
                continue
            seen_dirs.add(norm)
            try:
                remove_empty_dirs(dir_path, stop_at=watch_root)
            except Exception as err:
                logger.debug("Failed to clean empty dirs after batch delete: %s", err)

    deleted = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted, "files_deleted": files_deleted}


def delete_group_records(ids: list[int], group_dir: str, db) -> dict:
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

    rows = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(ids)).all()
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
    cleanup_root = resolve_group_cleanup_root(rows, folders_by_id)
    season_num = extract_group_season_number(group_dir, rows)

    expected_group = normcase_path(group_dir)
    files_to_delete = []
    for row in rows:
        row_group = normcase_path(record_output_group_dir(row))
        if row_group != expected_group:
            raise HTTPException(400, detail="分组记录与目标目录不匹配，已取消删除")
        files_to_delete.extend(record_output_cleanup_paths(row))

    other_rows = db.query(ScrapeRecord).filter(~ScrapeRecord.id.in_(ids)).all()
    blocked_files = set()
    for row in other_rows:
        row_group = normcase_path(record_output_group_dir(row))
        if row_group != expected_group:
            continue
        for path in record_output_cleanup_paths(row):
            blocked_files.add(normcase_path(path))

    can_touch_group_dir = bool(files_to_delete)
    files_deleted = 0
    visited = set()
    for path in files_to_delete:
        norm_path = normcase_path(path)
        if not norm_path or norm_path in visited or norm_path in blocked_files:
            continue
        visited.add(norm_path)
        if safe_remove_file(path):
            files_deleted += 1

    if can_touch_group_dir:
        files_deleted += delete_dir_sidecars_only(group_dir)

    deleted = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
    dir_deleted = cleanup_empty_group_dir(group_dir) if can_touch_group_dir else False
    parent_files_deleted = 0
    parent_dir_deleted = False
    if can_touch_group_dir:
        parent_files_deleted, parent_dir_deleted = cleanup_parent_show_dir(
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


def clear_failed_records(db) -> dict:
    deleted = db.query(ScrapeRecord).filter(ScrapeRecord.status == "failed").delete(synchronize_session=False)
    db.commit()
    return {"ok": True, "deleted": deleted}


def clear_all_records(db, vacuum_fn) -> dict:
    deleted = db.query(ScrapeRecord).delete(synchronize_session=False)
    db.commit()
    db.close()
    vacuum_fn()
    return {"ok": True, "deleted": deleted}
