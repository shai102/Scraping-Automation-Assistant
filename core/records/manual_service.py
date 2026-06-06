"""Manual match, retry, restore, and metadata refresh services for records."""

import json
import logging
import os
import shutil
import threading
import time
import uuid
from collections import defaultdict
from typing import Optional

from fastapi import HTTPException

from core.models.media_item import MediaItem
from core.metadata.local_hub_service import (
    MetadataHubError,
    update_record_from_metadata_hub,
)
from core.services.worker_context import WorkerContext
from core.settings.config_service import get_metadata_hub_root
from core.workers.task_runner import process_task as process_task_impl
from db.scrape_models import MonitorFolder, ScrapeRecord
from db.tmdb_api import fetch_bgm_by_id, fetch_tmdb_by_id
from monitor.delete_sync import remove_empty_dirs
from monitor.metadata_refresh import (
    record_to_dict as record_to_dict_impl,
    refresh_record_metadata as refresh_record_metadata_impl,
)
from utils.cache import invalidate_cache_prefix

from .delete_service import (
    DIR_SIDECAR_EXACT,
    DIR_SIDECAR_PATTERNS,
    MEDIA_EXTS,
)

logger = logging.getLogger(__name__)


def _get_watcher():
    from server import get_watcher

    return get_watcher()


def _broadcast_record_update(row: ScrapeRecord):
    watcher = _get_watcher()
    if watcher and watcher._broadcast:
        watcher._broadcast({"type": "record_update", "data": record_to_dict_impl(row)})


def _notify_manual_success(folder, item):
    watcher = _get_watcher()
    if watcher and hasattr(watcher, "_tg_batcher") and folder:
        watcher._tg_batcher.add(folder.id, os.path.basename(folder.path), item)


def _delete_file_sidecars(file_path: str):
    stem = os.path.splitext(file_path)[0]
    for suffix in (".nfo", "-thumb.jpg"):
        path = stem + suffix
        if os.path.isfile(path):
            try:
                os.remove(path)
                logger.debug(f"Deleted sidecar: {path}")
            except Exception as err:
                logger.warning(f"Failed to delete sidecar {path}: {err}")


def _cleanup_dir_sidecars(target_file: str, watch_root: Optional[str] = None):
    def _has_media(directory: str) -> bool:
        for dirpath, _, filenames in os.walk(directory):
            for filename in filenames:
                if filename.lower().endswith(MEDIA_EXTS):
                    return True
        return False

    def _safe_remove(file_path: str):
        try:
            os.remove(file_path)
            logger.debug(f"Deleted dir sidecar: {file_path}")
        except Exception as err:
            logger.warning(f"Failed to delete dir sidecar {file_path}: {err}")

    def _delete_dir_level_sidecars(directory: str):
        try:
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                if not os.path.isfile(file_path):
                    continue
                filename_lower = filename.lower()
                if filename_lower in DIR_SIDECAR_EXACT:
                    _safe_remove(file_path)
                    continue
                for prefix, suffixes in DIR_SIDECAR_PATTERNS:
                    if filename_lower.startswith(prefix) and filename_lower.endswith(suffixes):
                        _safe_remove(file_path)
                        break
        except Exception as err:
            logger.warning(f"Failed to list dir for sidecar cleanup {directory}: {err}")

    target_dir = os.path.normpath(os.path.dirname(target_file))
    target_parent = os.path.normpath(os.path.dirname(target_dir))

    if not _has_media(target_dir):
        _delete_dir_level_sidecars(target_dir)

    if target_parent != target_dir and (
        watch_root is None or os.path.normcase(target_parent) != os.path.normcase(watch_root)
    ):
        if not _has_media(target_parent):
            _delete_dir_level_sidecars(target_parent)


def restore_record_file(row: ScrapeRecord, folder, db):
    target = row.target_path
    if not target or not os.path.exists(target):
        if not os.path.isfile(row.original_path):
            raise HTTPException(400, detail="源文件不存在，无法恢复")
        return

    organize_mode = getattr(folder, "organize_mode", "move") or "move" if folder else "move"
    _delete_file_sidecars(target)

    if os.path.normcase(os.path.normpath(target)) != os.path.normcase(os.path.normpath(row.original_path)):
        watch_root = os.path.normpath(folder.path) if folder else None
        if organize_mode in ("move", "rename"):
            original_dir = os.path.dirname(row.original_path)
            os.makedirs(original_dir, exist_ok=True)
            shutil.move(target, row.original_path)
            logger.info(f"Restored: {target} -> {row.original_path}")
            _cleanup_dir_sidecars(target, watch_root=watch_root)
            remove_empty_dirs(os.path.dirname(target), stop_at=watch_root)
        else:
            try:
                os.remove(target)
                logger.info(f"Removed target copy/link: {target}")
                _cleanup_dir_sidecars(target, watch_root=watch_root)
                remove_empty_dirs(os.path.dirname(target), stop_at=watch_root)
            except Exception as err:
                logger.warning(f"Failed to remove target {target}: {err}")

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


def archive_file(item, row, folder, ctx, tid, provider, db):
    organize_mode = getattr(folder, "organize_mode", "move") or "move" if folder else "move"

    if organize_mode == "rename" and folder:
        ctx.target_root.set(folder.path)

    target = item.full_target or os.path.join(item.dir, item.new_name_only or item.old_name)
    target_dir = os.path.dirname(target)
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    if os.path.normcase(item.path) != os.path.normcase(target):
        same_file = False
        if os.path.exists(target) and os.path.isfile(item.path):
            try:
                same_file = os.path.samefile(item.path, target)
            except (OSError, ValueError):
                pass

        if same_file:
            item.path = target
        elif os.path.exists(target):
            if not os.path.isfile(item.path):
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
            if organize_mode == "copy":
                shutil.copy2(item.path, target)
            elif organize_mode == "symlink":
                os.symlink(os.path.abspath(item.path), target)
            elif organize_mode == "hardlink":
                os.link(item.path, target)
            else:
                shutil.move(item.path, target)

            if organize_mode not in ("copy", "symlink", "hardlink"):
                item.path = target
                watch_root = os.path.normpath(folder.path) if folder else None
                remove_empty_dirs(src_dir, stop_at=watch_root)
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


def process_single_manual(row, body, folder, db):
    if row.target_path and os.path.exists(row.target_path):
        restore_record_file(row, folder, db)
        db.commit()

    if not os.path.isfile(row.original_path):
        row.status = "failed"
        row.error_msg = "源文件不存在"
        db.commit()
        raise HTTPException(400, detail="源文件不存在")

    if body.provider == "tmdb" and body.is_tv:
        invalidate_cache_prefix(f"tmdb_ep_v3:{body.candidate_id}_")

    ctx = WorkerContext()
    if body.provider == "tmdb":
        _title, tid, _msg, meta = fetch_tmdb_by_id(body.candidate_id, body.is_tv, ctx.tmdb_api_key.get())
    else:
        _title, tid, _msg, meta = fetch_bgm_by_id(body.candidate_id, ctx.bgm_api_key.get())

    if tid == "None":
        raise HTTPException(400, detail="候选 ID 无效")

    item = MediaItem(
        id=str(uuid.uuid4()),
        path=row.original_path,
        dir=os.path.dirname(row.original_path),
        old_name=row.original_name,
        ext=os.path.splitext(row.original_name)[1],
    )

    organize_mode = getattr(folder, "organize_mode", "move") or "move" if folder else "move"
    if organize_mode == "rename" and folder:
        ctx.target_root.set(folder.path)
        ctx.preserve_existing_folder.set(getattr(folder, "preserve_existing_folder", False))
    elif folder and folder.target_root:
        ctx.target_root.set(folder.target_root)
    if folder and folder.data_source:
        ctx.source_var.set(folder.data_source)

    ctx.media_type_override.set("电视剧" if body.is_tv else "电影")
    ctx.manual_locks[item.path] = (
        body.candidate_title,
        str(body.candidate_id),
        f"手动/{body.provider}命中",
        meta or {},
    )
    if body.season_override is not None:
        ctx.forced_seasons[item.path] = body.season_override
    if body.episode_offset != 0:
        ctx.forced_offsets[item.path] = body.episode_offset
    ctx.file_list = [item]

    try:
        process_task_impl(ctx, 0)
    except Exception as err:
        logger.error(f"Manual match process_task failed: {err}")
        row.status = "failed"
        row.error_msg = str(err)[:500]
        db.commit()
        raise HTTPException(500, detail=f"识别失败: {str(err)[:100]}")

    try:
        archive_file(item, row, folder, ctx, tid, body.provider, db)
        db.commit()
    except HTTPException:
        raise
    except Exception as err:
        logger.error(f"Manual match archive failed: {err}")
        row.status = "failed"
        row.error_msg = str(err)[:500]
        db.commit()
        raise HTTPException(500, detail=f"归档失败: {str(err)[:100]}")

    _broadcast_record_update(row)
    _notify_manual_success(folder, item)
    return row


def manual_match_record(record_id: int, body, db) -> dict:
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)

    folder = db.query(MonitorFolder).get(row.folder_id) if row.folder_id else None
    process_single_manual(row, body, folder, db)
    processed_count = 1

    if body.scope == "folder":
        original_dir = os.path.normpath(os.path.dirname(row.original_path))
        siblings = db.query(ScrapeRecord).filter(
            ScrapeRecord.folder_id == row.folder_id,
            ScrapeRecord.id != row.id,
        ).all()
        siblings = [
            sibling
            for sibling in siblings
            if os.path.normpath(os.path.dirname(sibling.original_path)) == original_dir
        ]
        for sibling in siblings:
            try:
                process_single_manual(sibling, body, folder, db)
                processed_count += 1
            except Exception as err:
                logger.error(f"Folder-scope manual match failed for {sibling.original_path}: {err}")

    return {"ok": True, "processed": processed_count}


def retry_record_async(record_id: int, db) -> dict:
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

    watcher = _get_watcher()

    def _run():
        if watcher:
            watcher._process_file(row.original_path)

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "message": "重试已启动"}


def batch_retry_records(ids: list[int], db) -> dict:
    rows = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(ids)).all()
    paths_to_retry = [row.original_path for row in rows if os.path.isfile(row.original_path)]

    for row in rows:
        db.delete(row)
    db.commit()

    watcher = _get_watcher()
    count = 0
    if watcher:
        dir_groups = defaultdict(list)
        for path in paths_to_retry:
            dir_groups[os.path.dirname(path)].append(path)

        for _dir_path, paths in dir_groups.items():
            for path in sorted(paths):
                norm = os.path.normpath(path)
                with watcher._pending_lock:
                    watcher._processed.discard(norm)
                watcher._pool.submit(watcher._process_file, path)
                with watcher._pending_lock:
                    watcher._processed.add(norm)
                count += 1
                time.sleep(0.1)

    return {"ok": True, "count": count}


def refresh_metadata_for_record(record_id: int, db) -> dict:
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)
    if row.status != "success":
        raise HTTPException(400, detail="只能刷新已成功的记录")
    if not row.matched_id or not row.metadata_json:
        raise HTTPException(400, detail="记录缺少匹配信息")

    watcher = _get_watcher()
    worker_ctx = watcher._worker_ctx if watcher else WorkerContext()
    broadcast_fn = watcher._broadcast if watcher else (lambda _data: None)

    try:
        updated = refresh_record_metadata_impl(row, db, worker_ctx, broadcast_fn)
        if updated:
            broadcast_fn({"type": "record_update", "data": record_to_dict_impl(row)})
            return {"ok": True, "message": "元数据已刷新", "updated": True}
        return {"ok": True, "message": "元数据已是最新，无需刷新", "updated": False}
    except Exception as err:
        logger.error(f"Metadata refresh failed for record {record_id}: {err}")
        raise HTTPException(500, detail=f"刷新失败: {str(err)[:200]}")


def batch_refresh_metadata_records(ids: list[int], db) -> dict:
    rows = db.query(ScrapeRecord).filter(
        ScrapeRecord.id.in_(ids),
        ScrapeRecord.status == "success",
        ScrapeRecord.matched_id.isnot(None),
        ScrapeRecord.metadata_json.isnot(None),
    ).all()

    if not rows:
        return {"ok": True, "total": 0, "updated": 0, "message": "没有符合条件的记录"}

    watcher = _get_watcher()
    worker_ctx = watcher._worker_ctx if watcher else WorkerContext()
    broadcast_fn = watcher._broadcast if watcher else (lambda _data: None)

    updated_count = 0
    for row in rows:
        try:
            if refresh_record_metadata_impl(row, db, worker_ctx, broadcast_fn):
                updated_count += 1
                broadcast_fn({"type": "record_update", "data": record_to_dict_impl(row)})
        except Exception as err:
            logger.warning(f"Batch metadata refresh failed for record {row.id}: {err}")
        time.sleep(1.0)

    return {
        "ok": True,
        "total": len(rows),
        "updated": updated_count,
        "message": f"已刷新 {updated_count}/{len(rows)} 条记录",
    }


def update_metadata_from_hub_for_record(record_id: int, db) -> dict:
    row = db.query(ScrapeRecord).get(record_id)
    if not row:
        raise HTTPException(404)

    root_path = get_metadata_hub_root()
    try:
        result = update_record_from_metadata_hub(row, root_path)
    except MetadataHubError as err:
        raise HTTPException(400, detail=str(err)) from err
    except Exception as err:
        logger.exception("Metadata Hub update failed for record %s", record_id)
        raise HTTPException(500, detail=f"从 Metadata Hub 更新失败: {str(err)[:200]}") from err

    db.commit()
    _broadcast_record_update(row)
    logger.info(
        "Metadata Hub 更新: record_id=%s | tmdb_id=%s | target_path=%s | copied=%s",
        row.id,
        result["tmdb_id"],
        result["target_path"],
        len(result["copied"]),
    )
    return {
        "ok": True,
        "updated": True,
        "message": f"已从 Metadata Hub 更新 {len(result['copied'])} 个元数据文件",
        "result": result,
    }


def batch_update_metadata_from_hub_records(ids: list[int], db) -> dict:
    rows = (
        db.query(ScrapeRecord)
        .filter(
            ScrapeRecord.id.in_(ids),
            ScrapeRecord.status == "success",
            ScrapeRecord.matched_provider == "tmdb",
        )
        .all()
    )
    if not rows:
        return {"ok": True, "total": 0, "updated": 0, "failed": 0, "message": "没有符合条件的 TMDB 记录"}

    root_path = get_metadata_hub_root()
    updated = 0
    errors = []
    for row in rows:
        try:
            result = update_record_from_metadata_hub(row, root_path)
            updated += 1
            logger.info(
                "Metadata Hub 更新: record_id=%s | tmdb_id=%s | target_path=%s | copied=%s",
                row.id,
                result["tmdb_id"],
                result["target_path"],
                len(result["copied"]),
            )
            _broadcast_record_update(row)
        except Exception as err:
            errors.append({"id": row.id, "message": str(err)})
            logger.warning("Metadata Hub batch update failed for record %s: %s", row.id, err)

    db.commit()
    return {
        "ok": True,
        "total": len(rows),
        "updated": updated,
        "failed": len(errors),
        "errors": errors[:20],
        "message": f"已从 Metadata Hub 更新 {updated}/{len(rows)} 条记录",
    }
