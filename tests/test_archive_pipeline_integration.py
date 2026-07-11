import os
import tempfile
import unittest
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from db.database import Base
from db.scrape_models import ArchiveOperation, MonitorFolder, ScrapeRecord
from monitor.file_processor_archive import finalize_processed_item


class ArchivePipelineIntegrationTests(unittest.TestCase):
    def test_recognized_item_archives_writes_sidecar_and_journal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source_root = os.path.join(tmpdir, "incoming")
            target_root = os.path.join(tmpdir, "library")
            os.makedirs(source_root)
            source = os.path.join(source_root, "raw.mkv")
            target = os.path.join(target_root, "Show [tmdbid=1]", "Season 1", "Show-S01E01.mkv")
            with open(source, "wb") as handle:
                handle.write(b"video")

            engine = create_engine(
                "sqlite://",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
            Base.metadata.create_all(engine)
            Session = sessionmaker(bind=engine)
            db = Session()
            try:
                folder = MonitorFolder(path=source_root, target_root=target_root, organize_mode="move")
                db.add(folder)
                db.flush()
                record = ScrapeRecord(
                    folder_id=folder.id,
                    original_path=source,
                    original_name="raw.mkv",
                    status="processing",
                )
                db.add(record)
                db.commit()

                sidecars = []
                watcher = SimpleNamespace(
                    _worker_ctx=SimpleNamespace(
                        _write_sidecar_files=lambda _item, path: sidecars.append(path + ".nfo")
                    ),
                    _broadcast=lambda _payload: None,
                    _tg_batcher=SimpleNamespace(add=lambda *_args: None),
                    _emby_notifier=SimpleNamespace(notify_success=lambda *_args: None),
                    _last_success_at=None,
                )
                item = SimpleNamespace(
                    path=source,
                    full_target=target,
                    dir=source_root,
                    new_name_only=os.path.basename(target),
                    metadata={"id": "1", "title": "Show", "provider": "tmdb"},
                )

                self.assertTrue(finalize_processed_item(watcher, folder, db, record, item, source))
                db.refresh(record)
                operation = db.query(ArchiveOperation).one()
                self.assertTrue(os.path.isfile(target))
                self.assertFalse(os.path.exists(source))
                self.assertEqual([target + ".nfo"], sidecars)
                self.assertEqual("success", record.status)
                self.assertEqual("completed", operation.status)
            finally:
                db.close()
                engine.dispose()


if __name__ == "__main__":
    unittest.main()
