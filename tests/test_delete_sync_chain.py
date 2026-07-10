import os
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from db.database import Base
from db.scrape_models import MonitorFolder, ScrapeRecord, SymlinkRecord
from monitor.delete_sync import DeleteSyncService
from monitor.scan_service import find_folder_for_path


class _WatcherStub:
    _folder_model = MonitorFolder

    def __init__(self):
        self._pending_lock = threading.Lock()
        self._processed = set()
        self.broadcasts = []

    def _find_folder(self, path, db):
        return find_folder_for_path(path, db)

    def _broadcast(self, payload):
        self.broadcasts.append(payload)


class DeleteSyncChainTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source_root = self.root / "source"
        self.export_root = self.root / "export"
        self.downstream_root = self.export_root / "tv"
        self.source_root.mkdir()
        self.downstream_root.mkdir(parents=True)

        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        self.Session = sessionmaker(bind=engine)
        self.engine = engine

    def tearDown(self):
        self.engine.dispose()
        self.temp_dir.cleanup()

    def _register_chain(self, source_path, exported_link, target_path):
        return self._register_chains(((source_path, exported_link, target_path),))

    def _register_chains(self, chains):
        db = self.Session()
        try:
            export_folder = MonitorFolder(
                path=str(self.source_root),
                target_root=str(self.export_root),
                organize_mode="symlink_export",
            )
            downstream_folder = MonitorFolder(
                path=str(self.downstream_root),
                target_root="",
                organize_mode="rename",
            )
            db.add_all((export_folder, downstream_folder))
            db.flush()
            for source_path, exported_link, target_path in chains:
                db.add(
                    SymlinkRecord(
                        folder_id=export_folder.id,
                        original_path=str(source_path),
                        link_path=str(exported_link),
                        status="success",
                    )
                )
                db.add(
                    ScrapeRecord(
                        folder_id=downstream_folder.id,
                        original_path=str(exported_link),
                        original_name=exported_link.name,
                        target_path=str(target_path),
                        status="success",
                    )
                )
            db.commit()
        finally:
            db.close()

        watcher = _WatcherStub()
        watcher._processed.update(str(source_path) for source_path, _, _ in chains)
        service = DeleteSyncService(
            watcher,
            record_to_dict=lambda row: {},
            symlink_record_to_dict=lambda row: {},
        )
        return watcher, service

    def _delete_source(self, source_path, service):
        source_path.unlink()
        with patch("monitor.delete_sync_file.SessionLocal", self.Session):
            service.handle_file_deleted(str(source_path))

    def _delete_source_dir(self, source_dir, service):
        shutil.rmtree(source_dir)
        with patch("monitor.delete_sync_dir.SessionLocal", self.Session):
            service.handle_dir_deleted(str(source_dir))

    def _assert_chain_records_deleted(self):
        db = self.Session()
        try:
            self.assertEqual(0, db.query(SymlinkRecord).count())
            self.assertEqual(0, db.query(ScrapeRecord).count())
        finally:
            db.close()

    def test_deleting_last_export_source_removes_entire_title_tree(self):
        source_path = self.source_root / "tv" / "episode.strm"
        source_path.parent.mkdir()
        source_path.write_text("https://example.invalid/video", encoding="utf-8")

        exported_link = self.downstream_root / "episode.strm"
        os.symlink(source_path, exported_link)

        show_root = self.downstream_root / "Test Show [tmdbid=123]"
        season_root = show_root / "Season 1"
        season_root.mkdir(parents=True)
        target_path = season_root / "Test Show-S01E01.strm"
        shutil.move(exported_link, target_path)

        episode_nfo = target_path.with_suffix(".nfo")
        episode_thumb = target_path.with_name(target_path.stem + "-thumb.jpg")
        episode_nfo.write_text("<episodedetails/>", encoding="utf-8")
        episode_thumb.write_bytes(b"image")
        season_nfo = season_root / "season.nfo"
        tvshow_nfo = show_root / "tvshow.nfo"
        season_nfo.write_text("<season/>", encoding="utf-8")
        tvshow_nfo.write_text("<tvshow/>", encoding="utf-8")

        watcher, service = self._register_chain(source_path, exported_link, target_path)
        self._delete_source(source_path, service)

        self.assertFalse(os.path.lexists(target_path))
        self.assertFalse(episode_nfo.exists())
        self.assertFalse(episode_thumb.exists())
        self.assertFalse(season_nfo.exists())
        self.assertFalse(tvshow_nfo.exists())
        self.assertFalse(show_root.exists())
        self.assertTrue(self.downstream_root.exists())
        self.assertNotIn(str(source_path), watcher._processed)
        self.assertEqual("symlink_deleted", watcher.broadcasts[-1]["type"])
        self._assert_chain_records_deleted()

    def test_deleting_one_episode_preserves_sibling_media_and_shared_metadata(self):
        source_path = self.source_root / "tv" / "episode1.strm"
        sibling_source = self.source_root / "tv" / "episode2.custom"
        source_path.parent.mkdir()
        source_path.write_text("episode-1", encoding="utf-8")
        sibling_source.write_text("episode-2", encoding="utf-8")

        exported_link = self.downstream_root / "episode1.strm"
        os.symlink(source_path, exported_link)
        show_root = self.downstream_root / "Test Show [tmdbid=123]"
        season_root = show_root / "Season 1"
        season_root.mkdir(parents=True)
        target_path = season_root / "Test Show-S01E01.strm"
        sibling_target = season_root / "Test Show-S01E02.custom"
        shutil.move(exported_link, target_path)
        os.symlink(sibling_source, sibling_target)

        season_nfo = season_root / "season.nfo"
        tvshow_nfo = show_root / "tvshow.nfo"
        poster = show_root / "poster.jpg"
        season_nfo.write_text("<season/>", encoding="utf-8")
        tvshow_nfo.write_text("<tvshow/>", encoding="utf-8")
        poster.write_bytes(b"image")

        watcher, service = self._register_chain(source_path, exported_link, target_path)
        watcher._worker_ctx = SimpleNamespace(
            get_media_exts=lambda: (".strm", ".custom"),
        )
        self._delete_source(source_path, service)

        self.assertFalse(os.path.lexists(target_path))
        self.assertTrue(os.path.lexists(sibling_target))
        self.assertTrue(season_nfo.exists())
        self.assertTrue(tvshow_nfo.exists())
        self.assertTrue(poster.exists())
        self._assert_chain_records_deleted()

    def test_deleting_export_source_directory_removes_all_moved_titles(self):
        source_dir = self.source_root / "tv" / "release"
        source_dir.mkdir(parents=True)
        source_paths = [source_dir / "episode1.strm", source_dir / "episode2.strm"]
        for index, source_path in enumerate(source_paths, start=1):
            source_path.write_text(f"episode-{index}", encoding="utf-8")

        export_dir = self.downstream_root / "release"
        export_dir.mkdir()
        exported_links = [export_dir / path.name for path in source_paths]
        for source_path, exported_link in zip(source_paths, exported_links):
            os.symlink(source_path, exported_link)

        show_root = self.downstream_root / "Test Show [tmdbid=123]"
        season_root = show_root / "Season 1"
        season_root.mkdir(parents=True)
        target_paths = [
            season_root / "Test Show-S01E01.strm",
            season_root / "Test Show-S01E02.strm",
        ]
        for exported_link, target_path in zip(exported_links, target_paths):
            shutil.move(exported_link, target_path)
            target_path.with_suffix(".nfo").write_text("<episodedetails/>", encoding="utf-8")
        (season_root / "season.nfo").write_text("<season/>", encoding="utf-8")
        (show_root / "tvshow.nfo").write_text("<tvshow/>", encoding="utf-8")
        (show_root / "poster.jpg").write_bytes(b"poster")

        watcher, service = self._register_chains(
            tuple(zip(source_paths, exported_links, target_paths))
        )
        self._delete_source_dir(source_dir, service)

        self.assertFalse(show_root.exists())
        self.assertFalse(export_dir.exists())
        self.assertTrue(self.downstream_root.exists())
        self.assertEqual("dir_deleted", watcher.broadcasts[-1]["type"])
        self._assert_chain_records_deleted()

    def test_deleting_movie_source_removes_movie_artwork_and_title_tree(self):
        source_path = self.source_root / "movie" / "movie.strm"
        source_path.parent.mkdir()
        source_path.write_text("movie", encoding="utf-8")
        exported_link = self.downstream_root / "movie.strm"
        os.symlink(source_path, exported_link)

        movie_root = self.downstream_root / "Test Movie (2026) [tmdbid=456]"
        movie_root.mkdir()
        target_path = movie_root / "Test Movie (2026).strm"
        shutil.move(exported_link, target_path)
        target_path.with_suffix(".nfo").write_text("<movie/>", encoding="utf-8")
        (movie_root / "poster.jpg").write_bytes(b"poster")
        (movie_root / "fanart.jpg").write_bytes(b"fanart")

        _watcher, service = self._register_chain(source_path, exported_link, target_path)
        self._delete_source(source_path, service)

        self.assertFalse(movie_root.exists())
        self.assertTrue(self.downstream_root.exists())
        self._assert_chain_records_deleted()


if __name__ == "__main__":
    unittest.main()
