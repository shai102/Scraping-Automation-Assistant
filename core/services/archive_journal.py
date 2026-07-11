"""Persistence and conservative recovery for archive operations."""

import datetime
import logging
import os

from db.scrape_models import ArchiveOperation, ScrapeRecord

logger = logging.getLogger(__name__)


class ArchiveJournal:
    def __init__(self, db, operation: ArchiveOperation):
        self.db = db
        self.operation = operation

    @classmethod
    def begin(cls, db, *, record_id, source: str, target: str, organize_mode: str):
        operation = ArchiveOperation(
            record_id=record_id,
            source_path=os.path.normpath(source),
            target_path=os.path.normpath(target),
            organize_mode=organize_mode,
            phase="prepared",
            status="running",
        )
        db.add(operation)
        db.commit()
        db.refresh(operation)
        return cls(db, operation)

    def mark(self, phase: str, _details=None):
        self.operation.phase = phase
        self.operation.updated_at = datetime.datetime.now()
        self.db.commit()

    def complete(self):
        now = datetime.datetime.now()
        self.operation.phase = "completed"
        self.operation.status = "completed"
        self.operation.completed_at = now
        self.operation.updated_at = now
        self.operation.error_msg = None
        self.db.commit()

    def fail(self, error):
        self.operation.status = "failed"
        self.operation.error_msg = str(error)[:1000]
        self.operation.updated_at = datetime.datetime.now()
        self.db.commit()


def recover_incomplete_archive_operations(db) -> int:
    rows = db.query(ArchiveOperation).filter(ArchiveOperation.status == "running").all()
    recovered = 0
    for operation in rows:
        source_exists = os.path.exists(operation.source_path)
        target_exists = os.path.exists(operation.target_path)
        operation.status = "recovered"
        operation.updated_at = datetime.datetime.now()
        if target_exists:
            operation.phase = "target_present"
            operation.error_msg = "检测到上次归档中断，目标文件存在，等待元数据修复"
            if operation.record_id:
                record = db.get(ScrapeRecord, operation.record_id)
                if record and record.status != "success":
                    record.status = "failed"
                    record.target_path = operation.target_path
                    record.error_msg = operation.error_msg
        elif source_exists:
            operation.phase = "source_present"
            operation.error_msg = "检测到上次归档在文件操作前中断，可安全重新处理"
            if operation.record_id:
                record = db.get(ScrapeRecord, operation.record_id)
                if record and record.status == "processing":
                    record.status = "failed"
                    record.error_msg = operation.error_msg
        else:
            operation.phase = "paths_missing"
            operation.error_msg = "检测到上次归档中断，但源文件和目标文件均不存在"
            if operation.record_id:
                record = db.get(ScrapeRecord, operation.record_id)
                if record and record.status != "success":
                    record.status = "failed"
                    record.error_msg = operation.error_msg
        recovered += 1
    if recovered:
        db.commit()
        logger.warning("Recovered incomplete archive operations: %s", recovered)
    return recovered


def cleanup_archive_operations(db, *, retention_days: int = 30) -> int:
    cutoff = datetime.datetime.now() - datetime.timedelta(days=max(1, int(retention_days)))
    count = (
        db.query(ArchiveOperation)
        .filter(
            ArchiveOperation.status.in_(("completed", "failed", "recovered")),
            ArchiveOperation.updated_at < cutoff,
        )
        .delete(synchronize_session=False)
    )
    if count:
        db.commit()
    return int(count or 0)
