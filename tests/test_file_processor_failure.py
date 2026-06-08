import json
import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from monitor.file_processor import process_file


class SimpleVar:
    def __init__(self, value=None):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class FakeQuery:
    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return None


class FakeSession:
    def __init__(self):
        self.added = []
        self.closed = False

    def query(self, *args, **kwargs):
        return FakeQuery()

    def add(self, item):
        self.added.append(item)

    def commit(self):
        pass

    def refresh(self, item):
        if getattr(item, "id", None) is None:
            item.id = len(self.added)

    def close(self):
        self.closed = True


class FakeBaseWorkerContext:
    def __init__(self):
        self._cfg = {}
        self.dir_cache = {}
        self.dir_parse_events = {}
        self.db_cache = {}
        self.db_resolution_events = {}
        self.embedding_cache = {}
        self.ollama_embed_endpoint = None
        self.cache_lock = threading.Lock()

    def get_sub_audio_exts(self):
        return (".srt", ".ass")


class FakeProcessingContext:
    def __init__(self, config=None):
        self._cfg = config or {}
        self.target_root = SimpleVar("")
        self.preserve_existing_folder = SimpleVar(False)
        self.source_var = SimpleVar("")
        self.media_type_override = SimpleVar("")
        self.file_list = []

    def get_sub_audio_exts(self):
        return (".srt", ".ass")


class FakeWatcher:
    def __init__(self):
        self._worker_ctx = FakeBaseWorkerContext()
        self.broadcasts = []
        self.released = []

    def _acquire_dir_slot(self, path):
        return os.path.dirname(path)

    def _release_dir_slot(self, dir_key):
        self.released.append(dir_key)

    def _broadcast(self, message):
        self.broadcasts.append(message)


class FileProcessorFailureTests(unittest.TestCase):
    def test_unmatched_recognition_becomes_pending_manual(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            media_path = os.path.join(tmpdir, "unknown-show.S01E01.strm")
            with open(media_path, "w", encoding="utf-8") as handle:
                handle.write("#EXTM3U\n")

            folder = SimpleNamespace(
                id=7,
                path=tmpdir,
                target_root=tmpdir,
                organize_mode="move",
                skip_if_scraped=False,
                preserve_existing_folder=False,
                data_source="siliconflow_tmdb",
                media_type="tv",
            )
            session = FakeSession()
            watcher = FakeWatcher()

            def fake_process_task(ctx, index):
                item = ctx.file_list[index]
                item.metadata = {
                    "id": "None",
                    "title": "unknown-show",
                    "provider": "tmdb",
                    "pending_reason": "无法自动匹配到媒体",
                    "parse_source": "guessit",
                }

            with patch("monitor.file_processor.SessionLocal", return_value=session), \
                patch("monitor.file_processor.find_folder_for_path", return_value=folder), \
                patch("monitor.file_processor.WorkerContext", FakeProcessingContext), \
                patch("monitor.file_processor.process_task_impl", fake_process_task):
                process_file(watcher, media_path)

            self.assertTrue(session.closed)
            self.assertEqual([tmpdir], watcher.released)
            self.assertEqual(1, len(session.added))

            record = session.added[0]
            self.assertEqual("pending_manual", record.status)
            self.assertEqual("无法自动匹配到媒体", record.error_msg)
            self.assertEqual("unknown-show", record.matched_title)
            self.assertEqual("tmdb", record.matched_provider)
            self.assertEqual("guessit", json.loads(record.metadata_json)["parse_source"])
            self.assertTrue(any(msg["type"] == "record_update" for msg in watcher.broadcasts))


if __name__ == "__main__":
    unittest.main()
