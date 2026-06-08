import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.records import manual_service
from core.symlinks import action_service


class FakeQuery:
    def __init__(self, row=None):
        self.row = row

    def get(self, _id):
        return self.row


class FakeSession:
    def __init__(self, row=None):
        self.row = row
        self.deleted = []
        self.commits = 0

    def query(self, *args, **kwargs):
        return FakeQuery(self.row)

    def delete(self, row):
        self.deleted.append(row)

    def commit(self):
        self.commits += 1


class FakeWatcher:
    def __init__(self):
        self._worker_ctx = object()


class RetryQueueTests(unittest.TestCase):
    def test_record_retry_uses_persistent_enqueue_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            media_path = os.path.join(tmpdir, "retry.mkv")
            self._write(media_path)
            row = SimpleNamespace(
                id=5,
                original_path=media_path,
                folder_id=7,
                status="failed",
                error_msg="old",
            )
            session = FakeSession(row)
            watcher = FakeWatcher()

            with patch.object(manual_service, "_get_watcher", return_value=watcher), \
                patch.object(manual_service, "enqueue_path", return_value=123) as enqueue:
                result = manual_service.retry_record_async(row.id, session)

            self.assertTrue(result["ok"])
            self.assertEqual("processing", row.status)
            self.assertIsNone(row.error_msg)
            enqueue.assert_called_once()
            kwargs = enqueue.call_args.kwargs
            self.assertIs(kwargs["db"], session)
            self.assertTrue(kwargs["immediate"])
            self.assertTrue(kwargs["force"])
            self.assertEqual("manual_retry", kwargs["source"])

    def test_symlink_retry_uses_persistent_enqueue_path(self):
        session = FakeSession()
        watcher = FakeWatcher()

        with patch("server.get_watcher", return_value=watcher), \
            patch.object(action_service, "enqueue_path", return_value=456) as enqueue:
            action_service._queue_retry_paths(session, [("/tmp/source.mkv", 9)])

        enqueue.assert_called_once()
        kwargs = enqueue.call_args.kwargs
        self.assertIs(kwargs["db"], session)
        self.assertTrue(kwargs["immediate"])
        self.assertTrue(kwargs["force"])
        self.assertEqual("symlink_retry", kwargs["source"])

    @staticmethod
    def _write(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("media")


if __name__ == "__main__":
    unittest.main()
