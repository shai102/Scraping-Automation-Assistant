import datetime
import logging
import time

from core.metadata.completeness import metadata_is_incomplete, metadata_missing_fields
from db.database import SessionLocal
from db.scrape_models import ScrapeRecord
from monitor.metadata_refresh_records import record_to_dict
from monitor.metadata_refresh_update import refresh_record_metadata


logger = logging.getLogger(__name__)


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
    cutoff = datetime.datetime.now() - datetime.timedelta(days=lookback_days)

    db = SessionLocal()
    try:
        rows = (
            db.query(ScrapeRecord)
            .filter(
                ScrapeRecord.status == "success",
                ScrapeRecord.updated_at >= cutoff,
                ScrapeRecord.metadata_json.isnot(None),
                ScrapeRecord.matched_id.isnot(None),
            )
            .order_by(ScrapeRecord.id.desc())
            .all()
        )
        incomplete = [row for row in rows if metadata_is_incomplete(row.metadata_json or "")]
        if not incomplete:
            return

        logger.info(f"元数据巡检: 发现 {len(incomplete)} 条不完整记录，开始刷新")

        refreshed = 0
        for record in incomplete:
            if not running_check():
                break
            title = str(record.matched_title or record.original_path or "").strip()
            target_path = str(record.target_path or "").strip()
            missing_fields = metadata_missing_fields(record.metadata_json or "")
            logger.info(
                "元数据巡检项: "
                f"record_id={record.id} | title={title or '-'} | "
                f"target_path={target_path or '-'} | "
                f"missing_fields={','.join(missing_fields) or '-'}"
            )
            try:
                updated = refresh_record_metadata(record, db, worker_ctx, broadcast)
                if updated:
                    refreshed += 1
                    broadcast({"type": "record_update", "data": record_to_dict(record)})
            except Exception as err:
                logger.warning(
                    "元数据刷新失败: "
                    f"record_id={record.id} | title={title or '-'} | "
                    f"target_path={target_path or '-'} | reason={err}"
                )
            time.sleep(2.0)

        logger.info(f"元数据巡检完成: 刷新了 {refreshed}/{len(incomplete)} 条记录")
    finally:
        db.close()
