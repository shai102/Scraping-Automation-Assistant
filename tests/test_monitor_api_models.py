import unittest

from pydantic import ValidationError

from api.routes.monitor import FolderCreate, FolderUpdate


class MonitorApiModelTests(unittest.TestCase):
    def test_folder_create_accepts_known_modes(self):
        body = FolderCreate(
            path="/tmp",
            media_type="tv",
            data_source="bgm",
            organize_mode="symlink_export",
        )

        self.assertEqual("tv", body.media_type)
        self.assertEqual("bgm", body.data_source)
        self.assertEqual("symlink_export", body.organize_mode)

    def test_folder_update_rejects_unknown_modes(self):
        with self.assertRaises(ValidationError):
            FolderUpdate(organize_mode="archive")
        with self.assertRaises(ValidationError):
            FolderUpdate(media_type="anime")
        with self.assertRaises(ValidationError):
            FolderUpdate(data_source="unknown")


if __name__ == "__main__":
    unittest.main()
