from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from core.metadata.local_hub_service import (
    MetadataHubError,
    inspect_metadata_hub,
    update_record_from_metadata_hub,
)


class LocalMetadataHubTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.hub = self.root / "metadata hub"
        self.library = self.root / "library"
        self.hub.mkdir()
        self.library.mkdir()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_tv_update_copies_title_season_episode_and_images(self):
        source = self.hub / "测试剧 [tmdbid=123]"
        source_season = source / "Season 2"
        source_season.mkdir(parents=True)
        self._write(source / "tvshow.nfo", "<tvshow><uniqueid type=\"tmdb\">123</uniqueid></tvshow>")
        self._write(source / "poster.jpg", "hub-poster")
        self._write(source / "fanart.jpg", "hub-fanart")
        self._write(source_season / "season.nfo", "<season><seasonnumber>2</seasonnumber></season>")
        self._write(source_season / "season.jpg", "hub-season")
        self._write(
            source_season / "S02E03.nfo",
            "<episodedetails><season>2</season><episode>3</episode></episodedetails>",
        )
        self._write(source_season / "S02E03-thumb.jpg", "hub-thumb")

        target_season = self.library / "测试剧 [tmdbid=123]" / "Season 2"
        target_season.mkdir(parents=True)
        target_media = target_season / "测试剧 S02E03.mkv"
        self._write(target_media, "video")
        self._write(target_media.with_suffix(".nfo"), "old-nfo")
        self._write(target_season / "测试剧 S02E03-thumb.jpg", "old-thumb")

        record = SimpleNamespace(
            status="success",
            matched_provider="tmdb",
            matched_id="123",
            target_path=str(target_media),
            metadata_json=json.dumps({"type": "episode", "s": 2, "e": 3}),
        )
        result = update_record_from_metadata_hub(record, str(self.hub))

        target_root = target_season.parent
        self.assertEqual("hub-poster", (target_root / "poster.jpg").read_text())
        self.assertEqual("hub-fanart", (target_root / "fanart.jpg").read_text())
        self.assertEqual("hub-season", (target_root / "season02-poster.jpg").read_text())
        self.assertEqual("hub-season", (target_season / "folder.jpg").read_text())
        self.assertEqual("hub-thumb", (target_season / "测试剧 S02E03-thumb.jpg").read_text())
        self.assertIn("<episode>3</episode>", target_media.with_suffix(".nfo").read_text())
        self.assertEqual(9, len(result["copied"]))

    def test_movie_update_uses_tmdb_id_and_overwrites_sidecars(self):
        source = self.hub / "测试电影 (2026) [tmdbid=456]"
        source.mkdir()
        self._write(source / "movie.nfo", "<movie><uniqueid type=\"tmdb\">456</uniqueid></movie>")
        self._write(source / "poster.jpg", "movie-poster")
        self._write(source / "fanart.jpg", "movie-fanart")

        target_dir = self.library / "测试电影"
        target_dir.mkdir()
        target_media = target_dir / "测试电影.mkv"
        self._write(target_media, "video")
        record = SimpleNamespace(
            status="success",
            matched_provider="tmdb",
            matched_id="456",
            target_path=str(target_media),
            metadata_json=json.dumps({"type": "movie"}),
        )

        result = update_record_from_metadata_hub(record, str(self.hub))

        self.assertEqual("movie", result["media_type"])
        self.assertIn("<movie>", target_media.with_suffix(".nfo").read_text())
        self.assertEqual("movie-poster", (target_dir / "poster.jpg").read_text())
        self.assertEqual("movie-fanart", (target_dir / "fanart.jpg").read_text())

    def test_missing_tmdb_id_does_not_touch_target(self):
        target = self.library / "existing.mkv"
        target_nfo = self.library / "existing.nfo"
        self._write(target, "video")
        self._write(target_nfo, "keep-me")
        record = SimpleNamespace(
            status="success",
            matched_provider="tmdb",
            matched_id="999",
            target_path=str(target),
            metadata_json=json.dumps({"type": "movie"}),
        )

        with self.assertRaises(MetadataHubError):
            update_record_from_metadata_hub(record, str(self.hub))

        self.assertEqual("keep-me", target_nfo.read_text())

    def test_inspect_counts_tmdb_titles(self):
        (self.hub / "有ID [tmdbid=7]").mkdir()
        (self.hub / "无ID").mkdir()
        result = inspect_metadata_hub(str(self.hub))
        self.assertEqual(2, result["title_dirs"])
        self.assertEqual(1, result["indexed_titles"])

    @staticmethod
    def _write(path: Path, value: str):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
