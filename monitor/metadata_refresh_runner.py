import datetime
import logging
import time

from core.metadata.completeness import metadata_is_incomplete
from db.database import SessionLocal
from db.scrape_models import ScrapeRecord
from monitor import metadata_refresh_state
from monitor.metadata_refresh_policy import (
    metadata_refresh_options as _metadata_refresh_options,
    missing_fields_for_record as _missing_fields_for_record,
)
from monitor.metadata_refresh_records import record_to_dict
from monitor.metadata_refresh_update import refresh_record_metadata


logger = logging.getLogger(__name__)


def _get_or_create_state(db, record_id: int, now: datetime.datetime):
    return metadata_refresh_state.get_or_create_state(db, record_id, now)


def _should_skip_for_backoff(db, record_id: int, now: datetime.datetime) -> bool:
    return metadata_refresh_state.should_skip_for_backoff(db, record_id, now)


def _mark_refresh_result(
    db,
    record_id: int,
    *,
    before_missing: list[str],
    after_missing: list[str],
    updated: bool,
    error: str | None,
    now: datetime.datetime,
) -> None:
    return metadata_refresh_state.mark_refresh_result(
        db,
        record_id,
        before_missing=before_missing,
        after_missing=after_missing,
        updated=updated,
        error=error,
        now=now,
        get_state_fn=_get_or_create_state,
    )


def run_metadata_refresh_pass(
    worker_ctx,
    *,
    lookback_days: int,
    running_check,
    broadcast_fn=None,
) -> None:
    if not worker_ctx:
        return

    broadcast = broadcast_fn or (lambda _payload: None)
    options = _metadata_refresh_options(worker_ctx)
    now = datetime.datetime.now()
    cutoff = now - datetime.timedelta(days=lookback_days) if int(lookback_days or 0) > 0 else None

    db = SessionLocal()
    try:
        query = db.query(ScrapeRecord).filter(
            ScrapeRecord.status == "success",
            ScrapeRecord.metadata_json.isnot(None),
            ScrapeRecord.matched_id.isnot(None),
        )
        if cutoff is not None:
            query = query.filter(ScrapeRecord.updated_at >= cutoff)
        rows = query.order_by(ScrapeRecord.id.desc()).all()

        incomplete = [
            row
            for row in rows
            if metadata_is_incomplete(
                row.metadata_json or "",
                ignore_episode_title_rules=options.get("ignore_episode_title_rules"),
                skip_rules=options.get("skip_rules"),
                title_hint=row.matched_title or row.original_name or "",
                matched_id=row.matched_id or "",
                provider_hint=row.matched_provider or "",
            )
        ]
        if not incomplete:
            return

        eligible = []
        skipped_by_backoff = 0
        now = datetime.datetime.now()
        for record in incomplete:
            if _should_skip_for_backoff(db, record.id, now):
                skipped_by_backoff += 1
                continue
            eligible.append(record)

        if not eligible:
            logger.info(
                f"元数据巡检: 发现 {len(incomplete)} 条不完整记录，"
                f"{skipped_by_backoff} 条仍在冷却中，本轮无需刷新"
            )
            return

        suffix = f"，跳过冷却 {skipped_by_backoff} 条" if skipped_by_backoff else ""
        logger.info(f"元数据巡检: 发现 {len(incomplete)} 条不完整记录，开始刷新 {len(eligible)} 条{suffix}")

        refreshed = 0
        for record in eligible:
            if not running_check():
                break
            title = str(record.matched_title or record.original_path or "").strip()
            target_path = str(record.target_path or "").strip()
            missing_fields = _missing_fields_for_record(record, options)
            logger.info(
                "元数据巡检项: "
                f"record_id={record.id} | title={title or '-'} | "
                f"target_path={target_path or '-'} | "
                f"missing_fields={','.join(missing_fields) or '-'}"
            )
            try:
                attempt_started_at = datetime.datetime.now()
                updated = refresh_record_metadata(record, db, worker_ctx, broadcast)
                db.refresh(record)
                after_missing_fields = _missing_fields_for_record(record, options)
                _mark_refresh_result(
                    db,
                    record.id,
                    before_missing=missing_fields,
                    after_missing=after_missing_fields,
                    updated=updated,
                    error=None,
                    now=attempt_started_at,
                )
                if updated:
                    refreshed += 1
                    broadcast({"type": "record_update", "data": record_to_dict(record)})
            except Exception as err:
                db.rollback()
                _mark_refresh_result(
                    db,
                    record.id,
                    before_missing=missing_fields,
                    after_missing=missing_fields,
                    updated=False,
                    error=str(err),
                    now=datetime.datetime.now(),
                )
                logger.warning(
                    "元数据刷新失败: "
                    f"record_id={record.id} | title={title or '-'} | "
                    f"target_path={target_path or '-'} | reason={err}"
                )
            time.sleep(2.0)

        logger.info(f"元数据巡检完成: 刷新了 {refreshed}/{len(eligible)} 条记录")
    finally:
        db.close()
