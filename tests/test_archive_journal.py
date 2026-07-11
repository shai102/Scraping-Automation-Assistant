import os
import tempfile
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.services.archive_journal import ArchiveJournal, recover_incomplete_archive_operations
from db.database import Base
from db.scrape_models import ArchiveOperation, ScrapeRecord


class ArchiveJournalTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_journal_tracks_phases_and_completion(self):
        db = self.Session()
        try:
            journal = ArchiveJournal.begin(
                db, record_id=None, source="/tmp/source", target="/tmp/target", organize_mode="move"
            )
            journal.mark("file_done")
            journal.mark("sidecars_done")
            journal.complete()
            row = db.get(ArchiveOperation, journal.operation.id)
            self.assertEqual("completed", row.status)
            self.assertEqual("completed", row.phase)
        finally:
            db.close()

    def test_recovery_marks_target_present_record_repairable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source.mkv")
            target = os.path.join(tmpdir, "target.mkv")
            with open(target, "wb") as handle:
                handle.write(b"moved")
            db = self.Session()
            try:
                record = ScrapeRecord(
                    original_path=source,
                    original_name="source.mkv",
                    status="processing",
                )
                db.add(record)
                db.commit()
                journal = ArchiveJournal.begin(
                    db,
                    record_id=record.id,
                    source=source,
                    target=target,
                    organize_mode="move",
                )
                journal.mark("file_done")

                self.assertEqual(1, recover_incomplete_archive_operations(db))
                db.refresh(record)
                db.refresh(journal.operation)
                self.assertEqual("failed", record.status)
                self.assertEqual(target, record.target_path)
                self.assertEqual("recovered", journal.operation.status)
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
