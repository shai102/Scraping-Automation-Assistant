"""Single-file processing pipeline for FolderWatcher."""

import logging
import os
import json
import uuid

from sqlalchemy import or_

from core.models.media_item import MediaItem
from core.services.worker_context import WorkerContext
from core.workers.task_runner import process_task as process_task_impl
from db.database import SessionLocal
from db.scrape_models import ScrapeRecord, SymlinkRecord
from monitor.file_processor_archive import finalize_processed_item, handle_symlink_export
from monitor.file_processor_fastpath import try_nfo_fast_path
from monitor.file_stability import wait_for_file_stable
from monitor.record_state import (
    is_already_scraped,
    reset_scrape_record_for_rebuild,
    scrape_record_needs_repair,
    symlink_record_consumed_downstream,
    symlink_record_needs_repair,
    symlink_source_consumed_downstream,
)
from monitor.record_payloads import scrape_record_to_dict
from monitor.scan_service import find_folder_for_path
from monitor.task_queue import finish_task, finish_task_by_id, mark_task_running
from monitor.retry_policy import can_retry, classify_retryable_error, retry_delay_seconds, schedule_task_retry

logger = logging.getLogger(__name__)


def process_file(watcher, path: str, task_id: int | None = None):
    """Run the full recognition + archive pipeline for a single file."""
    if not os.path.isfile(path):
        finish_task_by_id(task_id, "failed", "源文件不存在")
        return

    task_status = None
    task_error = None
    task = None
    cfg = getattr(watcher._worker_ctx, "_cfg", {}) if watcher._worker_ctx else {}
    dir_slot = watcher._acquire_dir_slot(path)
    db = SessionLocal()

    def _set_task_result(status: str, error: str | None = None):
        nonlocal task_status, task_error
        task_status = status
        task_error = error

    try:
        task = mark_task_running(db, task_id)
        if cfg.get("file_stability_enabled", True):
            stable, stability_error = wait_for_file_stable(
                path,
                checks=cfg.get("file_stability_checks", 2),
                interval_seconds=cfg.get("file_stability_interval_seconds", 1.0),
            )
            if not stable:
                delay = retry_delay_seconds(
                    getattr(task, "attempts", 1),
                    cfg.get("retry_base_seconds", 30),
                    cfg.get("retry_max_seconds", 1800),
                )
                if can_retry(getattr(task, "attempts", 1), cfg.get("retry_max_attempts", 5)) and schedule_task_retry(watcher, path, task_id, stability_error, delay):
                    logger.info("文件暂未稳定，%s 秒后重试: %s", delay, path)
                    return
                _set_task_result("failed", stability_error)
                return
        folder = find_folder_for_path(path, db)
        record = None

        organize_mode_check = getattr(folder, "organize_mode", "move") or "move" if folder else "move"
        if organize_mode_check == "symlink_export":
            existing = db.query(SymlinkRecord).filter(
                or_(
                    SymlinkRecord.original_path == path,
                    SymlinkRecord.link_path == path,
                )
            ).first()
        else:
            existing = db.query(ScrapeRecord).filter(
                or_(
                    ScrapeRecord.original_path == path,
                    ScrapeRecord.target_path == path,
                )
            ).first()

        if existing:
            if organize_mode_check == "symlink_export":
                if (
                    symlink_record_needs_repair(existing)
                    and not symlink_record_consumed_downstream(existing, db, watcher._worker_ctx)
                ):
                    logger.info(f"检测到缺失的软链接产物，准备自动重建: {existing.original_path}")
                    db.delete(existing)
                    db.commit()
                else:
                    _set_task_result("skipped", "已有软链接记录，跳过重复处理")
                    return
            else:
                if scrape_record_needs_repair(existing, watcher._worker_ctx):
                    logger.info(f"检测到缺失的刮削产物，准备自动修复: {path}")
                    if existing.target_path and os.path.isfile(existing.target_path):
                        path = existing.target_path
                    record = existing
                    reset_scrape_record_for_rebuild(record)
                    db.commit()
                    watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
                elif str(getattr(existing, "status", "") or "").lower() in (
                    "failed",
                    "pending_manual",
                    "processing",
                ):
                    record = existing
                    reset_scrape_record_for_rebuild(record)
                    db.commit()
                    watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
                else:
                    _set_task_result("skipped", "已有刮削记录，跳过重复处理")
                    return

        is_sl_export_check = organize_mode_check == "symlink_export"
        sub_exts_skip = watcher._worker_ctx.get_sub_audio_exts() if watcher._worker_ctx else ()
        if (
            not is_sl_export_check
            and folder
            and getattr(folder, "skip_if_scraped", False)
            and is_already_scraped(path, sub_exts_skip)
        ):
            logger.info(f"跳过已有元数据（.nfo）的文件: {path}")
            record = ScrapeRecord(
                folder_id=folder.id,
                original_path=path,
                original_name=os.path.basename(path),
                status="skipped",
                error_msg="已有元数据（.nfo），跳过刮削",
            )
            db.add(record)
            db.commit()
            db.refresh(record)
            watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
            _set_task_result("skipped", record.error_msg)
            return

        from utils.title_parsing import is_decimal_episode

        pure_name = os.path.splitext(os.path.basename(path))[0]
        organize_mode_early = getattr(folder, "organize_mode", "move") or "move" if folder else "move"

        if is_decimal_episode(pure_name) and organize_mode_early != "symlink_export":
            logger.info(f"跳过小数集（总集篇）: {path}")
            record = ScrapeRecord(
                folder_id=folder.id if folder else None,
                original_path=path,
                original_name=os.path.basename(path),
                status="skipped",
                error_msg="小数集（总集篇），已跳过",
            )
            db.add(record)
            db.commit()
            db.refresh(record)
            watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
            _set_task_result("skipped", record.error_msg)
            return

        if organize_mode_early == "symlink_export" and folder:
            if symlink_source_consumed_downstream(folder, path, db, watcher._worker_ctx):
                logger.info(f"跳过已被下游刮削消费的导出源文件: {path}")
                _set_task_result("skipped", "已被下游刮削消费，跳过导出")
                return

        if organize_mode_early == "symlink_export" and folder:
            ok = handle_symlink_export(watcher, folder, path, db)
            _set_task_result("done" if ok else "failed", None if ok else "导出软链接失败")
            return

        if record is None:
            record = ScrapeRecord(
                folder_id=folder.id if folder else None,
                original_path=path,
                original_name=os.path.basename(path),
                status="processing",
            )
            db.add(record)
            db.commit()
            db.refresh(record)
            watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})

        if not watcher._worker_ctx:
            _set_task_result("failed", "WorkerContext 未初始化")
            return
        ctx = WorkerContext(config=dict(watcher._worker_ctx._cfg))
        ctx.dir_cache = watcher._worker_ctx.dir_cache
        ctx.dir_parse_events = watcher._worker_ctx.dir_parse_events
        ctx.db_cache = watcher._worker_ctx.db_cache
        ctx.db_resolution_events = watcher._worker_ctx.db_resolution_events
        ctx.embedding_cache = watcher._worker_ctx.embedding_cache
        ctx.ollama_embed_endpoint = watcher._worker_ctx.ollama_embed_endpoint
        ctx.cache_lock = watcher._worker_ctx.cache_lock

        if folder:
            if folder.target_root:
                ctx.target_root.set(folder.target_root)
            if getattr(folder, "organize_mode", "move") == "rename":
                ctx.target_root.set(folder.path)
            ctx.preserve_existing_folder.set(
                getattr(folder, "preserve_existing_folder", False)
            )
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
        logger.info(f"开始识别: {path}")

        nfo_fast_path_done = False
        sub_exts_fast_path = ctx.get_sub_audio_exts()
        if os.path.basename(path).lower().endswith(sub_exts_fast_path):
            nfo_fast_path_done = try_nfo_fast_path(item, ctx)

        if not nfo_fast_path_done:
            try:
                process_task_impl(ctx, 0)
            except Exception as err:
                logger.error(f"Recognition failed for {path}: {err}")
                record.status = "failed"
                record.error_msg = str(err)[:500]
                db.commit()
                watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
                retryable, _category = classify_retryable_error(err)
                if retryable and can_retry(getattr(task, "attempts", 1), cfg.get("retry_max_attempts", 5)):
                    delay = retry_delay_seconds(
                        getattr(task, "attempts", 1),
                        cfg.get("retry_base_seconds", 30),
                        cfg.get("retry_max_seconds", 1800),
                    )
                    if schedule_task_retry(watcher, path, task_id, record.error_msg, delay):
                        return
                _set_task_result("failed", record.error_msg)
                return

        tid = (item.metadata or {}).get("id", "None")
        if tid == "None" or not item.new_name_only:
            meta = item.metadata or {}
            logger.warning(
                "识别失败: %s 未匹配到 %s 媒体信息，跳过文件整理",
                path,
                "TMDB" if getattr(folder, "data_source", "siliconflow_tmdb") == "siliconflow_tmdb" else "BGM",
            )
            record.matched_title = meta.get("title")
            record.matched_provider = meta.get("provider")
            record.metadata_json = json.dumps(meta, ensure_ascii=False)
            if meta.get("error_code") == "rate_limited":
                record.status = "failed"
                record.error_msg = meta.get("error_msg") or "AI接口限流，请稍后重试"
            else:
                record.status = "pending_manual"
                record.error_msg = meta.get("pending_reason") or "无法自动识别"
            db.commit()
            watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
            retryable, _category = classify_retryable_error(record.error_msg)
            if retryable and can_retry(getattr(task, "attempts", 1), cfg.get("retry_max_attempts", 5)):
                delay = retry_delay_seconds(
                    getattr(task, "attempts", 1),
                    cfg.get("retry_base_seconds", 30),
                    cfg.get("retry_max_seconds", 1800),
                )
                if schedule_task_retry(watcher, path, task_id, record.error_msg, delay):
                    return
            _set_task_result("failed", record.error_msg)
            return

        meta = item.metadata or {}
        confidence = float(meta.get("confidence") or 0.0)
        if (
            cfg.get("recognition_confidence_gate_enabled", False)
            and confidence < float(cfg.get("recognition_confidence_threshold", 0.60))
        ):
            record.status = "pending_manual"
            record.matched_title = meta.get("title")
            record.matched_id = str(meta.get("id") or "None")
            record.matched_provider = meta.get("provider")
            record.metadata_json = json.dumps(meta, ensure_ascii=False)
            record.error_msg = (
                f"识别置信度 {confidence:.0%} 低于自动归档阈值 "
                f"{float(cfg.get('recognition_confidence_threshold', 0.60)):.0%}"
            )
            db.commit()
            watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
            _set_task_result("failed", record.error_msg)
            return
        logger.info(
            "识别完成: title=%s | path=%s | name=%s | year=%s | type=%s | season=%s | episode=%s | provider=%s | id=%s | parse_source=%s | resolution=%s | source=%s | video_codec=%s | audio_codec=%s | release_group=%s",
            meta.get("title") or "",
            path,
            item.new_name_only or "",
            meta.get("year") or "",
            meta.get("type") or "",
            meta.get("s") if meta.get("s") is not None else "",
            meta.get("e") if meta.get("e") is not None else "",
            meta.get("provider") or "",
            tid,
            meta.get("parse_source") or "",
            meta.get("resolution") or "",
            meta.get("source") or "",
            meta.get("video_codec") or "",
            meta.get("audio_codec") or "",
            meta.get("release_group") or "",
        )

        try:
            archived = finalize_processed_item(watcher, folder, db, record, item, path)
            _set_task_result("done" if archived else "failed", None if archived else record.error_msg)
        except Exception as err:
            logger.error(f"Archive failed for {path}: {err}")
            record.status = "failed"
            record.error_msg = str(err)[:500]
            db.commit()
            watcher._broadcast({"type": "record_update", "data": scrape_record_to_dict(record)})
            retryable, _category = classify_retryable_error(err)
            if retryable and can_retry(getattr(task, "attempts", 1), cfg.get("retry_max_attempts", 5)):
                delay = retry_delay_seconds(
                    getattr(task, "attempts", 1),
                    cfg.get("retry_base_seconds", 30),
                    cfg.get("retry_max_seconds", 1800),
                )
                if schedule_task_retry(watcher, path, task_id, record.error_msg, delay):
                    return
            _set_task_result("failed", record.error_msg)

    except Exception as err:
        logger.error(f"Unexpected error processing {path}: {err}")
        try:
            db.rollback()
        except Exception:
            pass
        retryable, _category = classify_retryable_error(err)
        if retryable and can_retry(getattr(task, "attempts", 1), cfg.get("retry_max_attempts", 5)):
            delay = retry_delay_seconds(1, cfg.get("retry_base_seconds", 30), cfg.get("retry_max_seconds", 1800))
            if schedule_task_retry(watcher, path, task_id, str(err)[:500], delay):
                return
        _set_task_result("failed", str(err)[:500])
    finally:
        if task_id and task_status:
            try:
                finish_task(db, task_id, task_status, task_error)
            except Exception as err:
                logger.debug("Failed to update task %s status: %s", task_id, err)
        db.close()
        watcher._release_dir_slot(dir_slot)
