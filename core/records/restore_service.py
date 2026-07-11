"""Restore archived record files before manual reprocessing."""

import logging
import os
import shutil
from typing import Optional

from fastapi import HTTPException

from db.scrape_models import ScrapeRecord
from monitor.delete_sync import remove_empty_dirs

from .delete_service import DIR_SIDECAR_EXACT, DIR_SIDECAR_PATTERNS, MEDIA_EXTS

logger = logging.getLogger(__name__)


def delete_file_sidecars(file_path: str):
    stem = os.path.splitext(file_path)[0]
    for suffix in (".nfo", "-thumb.jpg"):
        path = stem + suffix
        if os.path.isfile(path):
            try:
                os.remove(path)
            except Exception as err:
                logger.warning("Failed to delete sidecar %s: %s", path, err)


def cleanup_dir_sidecars(target_file: str, watch_root: Optional[str] = None):
    def has_media(directory: str) -> bool:
        return any(
            filename.lower().endswith(MEDIA_EXTS)
            for _dirpath, _dirs, filenames in os.walk(directory)
            for filename in filenames
        )

    def delete_level(directory: str):
        if not os.path.isdir(directory):
            return
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if not os.path.isfile(file_path):
                continue
            lower = filename.lower()
            matched = lower in DIR_SIDECAR_EXACT or any(
                lower.startswith(prefix) and lower.endswith(suffixes)
                for prefix, suffixes in DIR_SIDECAR_PATTERNS
            )
            if matched:
                try:
                    os.remove(file_path)
                except Exception as err:
                    logger.warning("Failed to delete dir sidecar %s: %s", file_path, err)

    target_dir = os.path.normpath(os.path.dirname(target_file))
    target_parent = os.path.normpath(os.path.dirname(target_dir))
    if not has_media(target_dir):
        delete_level(target_dir)
    if target_parent != target_dir and (
        watch_root is None or os.path.normcase(target_parent) != os.path.normcase(watch_root)
    ) and not has_media(target_parent):
        delete_level(target_parent)


def restore_record_file(row: ScrapeRecord, folder, db):
    target = row.target_path
    if not target or not os.path.exists(target):
        if not os.path.isfile(row.original_path):
            raise HTTPException(400, detail="源文件不存在，无法恢复")
        return

    organize_mode = getattr(folder, "organize_mode", "move") or "move" if folder else "move"
    delete_file_sidecars(target)
    if os.path.normcase(os.path.normpath(target)) != os.path.normcase(os.path.normpath(row.original_path)):
        watch_root = os.path.normpath(folder.path) if folder else None
        if organize_mode in ("move", "rename"):
            os.makedirs(os.path.dirname(row.original_path), exist_ok=True)
            shutil.move(target, row.original_path)
        else:
            try:
                os.remove(target)
            except Exception as err:
                logger.warning("Failed to remove target %s: %s", target, err)
        cleanup_dir_sidecars(target, watch_root=watch_root)
        remove_empty_dirs(os.path.dirname(target), stop_at=watch_root)

    if not os.path.isfile(row.original_path):
        raise HTTPException(400, detail="源文件恢复失败，文件不存在")
    row.target_path = None
    row.status = "processing"
    row.error_msg = None
    row.matched_title = None
    row.matched_id = None
    row.matched_provider = None
    row.metadata_json = None
    db.flush()
