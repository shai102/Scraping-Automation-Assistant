import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from monitor.scan_service import poll_once


class FakeWorkerContext:
    def get_media_exts(self):
        return (".mkv",)

    def get_sub_audio_exts(self):
        return ()


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return list(self.rows)


class FakeSession:
    def __init__(self, folders):
        self.folders = folders
        self.added = []
        self.commits = 0
        self.closed = False

    def query(self, model):
        if getattr(model, "__name__", "") == "MonitorFolder":
            return FakeQuery(self.folders)
        return FakeQuery([])

    def add(self, item):
        self.added.append(item)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class FakeWatcher:
    def __init__(self, *, max_enqueue=500):
        self._worker_ctx = FakeWorkerContext()
        self._pending = {}
        self._pending_lock = threading.Lock()
        self._processed = set()
        self._poll_max_enqueue_per_pass = max_enqueue
        self._poll_use_scan_state = False


class ScanServiceTests(unittest.TestCase):
    def test_poll_skips_nested_target_root(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target_root = os.path.join(tmpdir, "library")
            os.makedirs(target_root)
            incoming = os.path.join(tmpdir, "incoming.mkv")
            nested_output = os.path.join(target_root, "already-organized.mkv")
            self._write(incoming)
            self._write(nested_output)

            folder = self._folder(tmpdir, target_root=target_root)
            session = FakeSession([folder])
            watcher = FakeWatcher()

            with patch("monitor.scan_service.SessionLocal", return_value=session), \
                patch("monitor.scan_service.enqueue_task", side_effect=self._fake_enqueue_task):
                queued = poll_once(watcher)

            self.assertEqual(1, queued)
            self.assertIn(os.path.normpath(incoming), watcher._pending)
            self.assertNotIn(os.path.normpath(nested_output), watcher._pending)
            self.assertTrue(session.closed)

    def test_poll_limits_auto_enqueue_per_pass(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for index in range(4):
                self._write(os.path.join(tmpdir, f"item-{index}.mkv"))

            folder = self._folder(tmpdir)
            session = FakeSession([folder])
            watcher = FakeWatcher(max_enqueue=2)

            with patch("monitor.scan_service.SessionLocal", return_value=session), \
                patch("monitor.scan_service.enqueue_task", side_effect=self._fake_enqueue_task):
                queued = poll_once(watcher)

            self.assertEqual(2, queued)
            self.assertEqual(2, len(watcher._pending))

    @staticmethod
    def _folder(path, *, target_root=""):
        return SimpleNamespace(
            id=1,
            path=path,
            target_root=target_root,
            organize_mode="move",
            skip_if_scraped=False,
            enabled=True,
        )

    @staticmethod
    def _write(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("media")

    @staticmethod
    def _fake_enqueue_task(_db, _path, *, folder_id=None, task_type="scrape", source="poll"):
        return SimpleNamespace(id=1, status="queued", task_type=task_type), True


if __name__ == "__main__":
    unittest.main()
