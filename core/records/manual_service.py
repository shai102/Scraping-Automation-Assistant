"""Manual match, retry, restore, and metadata refresh services for records."""

import json
import logging
import os
import time
import uuid

from fastapi import HTTPException

from core.models.media_item import MediaItem
from core.metadata.local_hub_service import (
    MetadataHubError,
    update_record_from_metadata_hub,
)
from core.services.worker_context import WorkerContext
from core.services.archive_service import ArchiveConflictError, archive_service
from core.services.archive_journal import ArchiveJournal
from core.settings.config_service import get_metadata_hub_root
from core.workers.task_runner import process_task as process_task_impl
from db.scrape_models import MonitorFolder, ScrapeRecord
from db.tmdb_api import fetch_bgm_by_id, fetch_tmdb_by_id
from monitor.metadata_refresh import (
    record_to_dict as record_to_dict_impl,
    refresh_record_metadata as refresh_record_metadata_impl,
)
from monitor.scan_service import enqueue_path
from monitor.task_queue import enqueue_task
from utils.cache import invalidate_cache_prefix

from .restore_service import restore_record_file

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


def _enqueue_retry_path(path: str, db, *, folder_id: int | None = None, source: str = "manual_retry"):
    watcher = _get_watcher()
    if watcher and getattr(watcher, "_worker_ctx", None):
        return enqueue_path(
            watcher,
            path,
            source=source,
            immediate=True,
            force=True,
            db=db,
        )

    task, _created = enqueue_task(
        db,
        path,
        folder_id=folder_id,
        task_type="scrape",
        source=source,
    )
    return task.id


def archive_file(item, row, folder, ctx, tid, provider, db):
    organize_mode = getattr(folder, "organize_mode", "move") or "move" if folder else "move"

    if organize_mode == "rename" and folder:
        ctx.target_root.set(folder.path)

    target = item.full_target or os.path.join(item.dir, item.new_name_only or item.old_name)
    journal = ArchiveJournal.begin(
        db,
        record_id=getattr(row, "id", None),
        source=item.path,
        target=target,
        organize_mode=organize_mode,
    )
    try:
        result = archive_service.archive(
            item,
            target=target,
            organize_mode=organize_mode,
            write_sidecars=ctx._write_sidecar_files,
            watch_root=os.path.normpath(folder.path) if folder else None,
            allow_existing_target=not os.path.isfile(item.path),
            on_phase=journal.mark,
        )
        target = result.target
    except ArchiveConflictError as err:
        journal.fail(err)
        row.status = "failed"
        row.target_path = target
        row.error_msg = str(err)
        db.commit()
        raise HTTPException(400, detail=str(err)) from err
    except Exception as err:
        journal.fail(err)
        raise

    row.status = "success"
    row.matched_title = (item.metadata or {}).get("title")
    row.matched_id = str(tid)
    row.matched_provider = provider
    row.target_path = target
    row.metadata_json = json.dumps(item.metadata or {}, ensure_ascii=False)
    row.error_msg = None
    db.flush()
    journal.complete()
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
    row = db.get(ScrapeRecord, record_id)
    if not row:
        raise HTTPException(404)

    folder = db.get(MonitorFolder, row.folder_id) if row.folder_id else None
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
    row = db.get(ScrapeRecord, record_id)
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

    _enqueue_retry_path(row.original_path, db, folder_id=row.folder_id, source="manual_retry")
    return {"ok": True, "message": "重试已启动"}


def batch_retry_records(ids: list[int], db) -> dict:
    rows = db.query(ScrapeRecord).filter(ScrapeRecord.id.in_(ids)).all()
    retry_items = [
        (row.original_path, row.folder_id)
        for row in rows
        if os.path.isfile(row.original_path)
    ]

    for row in rows:
        db.delete(row)
    db.commit()

    count = 0
    for path, folder_id in sorted(retry_items, key=lambda item: item[0]):
        _enqueue_retry_path(path, db, folder_id=folder_id, source="batch_retry")
        count += 1

    return {"ok": True, "count": count}


def refresh_metadata_for_record(record_id: int, db) -> dict:
    row = db.get(ScrapeRecord, record_id)
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
    row = db.get(ScrapeRecord, record_id)
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
