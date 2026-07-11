import os
import tempfile
import unittest
from types import SimpleNamespace

from core.services.archive_service import ArchiveConflictError, ArchiveService
from core.services.worker_context import WorkerContext


class ArchiveServiceTests(unittest.TestCase):
    def test_move_and_sidecar_use_shared_service(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = os.path.join(tmpdir, "source")
            target_dir = os.path.join(tmpdir, "target")
            os.makedirs(source_dir)
            source = os.path.join(source_dir, "episode.mkv")
            target = os.path.join(target_dir, "Show-S01E01.mkv")
            with open(source, "wb") as handle:
                handle.write(b"video")
            item = SimpleNamespace(path=source)
            sidecars = []

            result = ArchiveService().archive(
                item,
                target=target,
                organize_mode="move",
                write_sidecars=lambda _item, path: sidecars.append(path),
                watch_root=source_dir,
            )

            self.assertEqual("move", result.operation)
            self.assertFalse(os.path.exists(source))
            self.assertTrue(os.path.isfile(target))
            self.assertEqual([target], sidecars)

    def test_existing_target_raises_typed_conflict(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source.mkv")
            target = os.path.join(tmpdir, "target.mkv")
            for path in (source, target):
                with open(path, "wb") as handle:
                    handle.write(path.encode())
            with self.assertRaises(ArchiveConflictError):
                ArchiveService().archive(
                    SimpleNamespace(path=source),
                    target=target,
                    organize_mode="move",
                    write_sidecars=lambda *_args: None,
                )

    def test_worker_context_emits_status_from_legacy_tree_calls(self):
        events = []
        ctx = WorkerContext(config={}, on_status=lambda *args: events.append(args))
        ctx.tree.set("record-1", "st", "识别中")
        self.assertEqual("record-1", events[0][0])
        self.assertEqual("识别中", events[0][1])


if __name__ == "__main__":
    unittest.main()
