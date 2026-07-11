import datetime
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from core.settings import config_service
from monitor.file_stability import wait_for_file_stable
from monitor.retry_policy import can_retry, classify_retryable_error, retry_delay_seconds
from monitor.runtime_maintenance import cleanup_old_logs


class RuntimeHardeningTests(unittest.TestCase):
    def test_file_stability_detects_changing_file(self):
        with patch("monitor.file_stability.time.sleep"), patch(
            "monitor.file_stability.file_snapshot",
            side_effect=[(10, 1), (11, 2)],
        ):
            stable, reason = wait_for_file_stable("video.mkv", checks=2)
        self.assertFalse(stable)
        self.assertIn("仍在写入", reason)

    def test_retry_policy_distinguishes_transient_and_permanent_errors(self):
        self.assertTrue(classify_retryable_error("HTTP 429 rate limit")[0])
        self.assertTrue(classify_retryable_error("connection timed out")[0])
        self.assertFalse(classify_retryable_error(PermissionError("denied"))[0])
        self.assertEqual(30, retry_delay_seconds(1, 30, 1800))
        self.assertEqual(120, retry_delay_seconds(3, 30, 1800))
        self.assertTrue(can_retry(4, 5))
        self.assertFalse(can_retry(5, 5))

    def test_config_migration_creates_backup_and_version(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "renamer_config.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"tmdb_api_key": "secret"}, handle)
            with patch.object(config_service, "CONFIG_FILE", path):
                cfg = config_service.load_settings()
            self.assertEqual(config_service.CONFIG_VERSION, cfg["config_version"])
            self.assertTrue(cfg["file_stability_enabled"])
            backups = [name for name in os.listdir(tmpdir) if ".bak-" in name]
            self.assertEqual(1, len(backups))

    def test_old_partitioned_logs_are_removed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = os.path.join(tmpdir, "logs", "app")
            os.makedirs(log_dir)
            old_date = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            old_path = os.path.join(log_dir, f"{old_date}.log")
            current_path = os.path.join(log_dir, f"{current_date}.log")
            open(old_path, "w", encoding="utf-8").close()
            open(current_path, "w", encoding="utf-8").close()
            removed = cleanup_old_logs(retention_days=30, data_dir=tmpdir)
            self.assertEqual(1, removed)
            self.assertFalse(os.path.exists(old_path))
            self.assertTrue(os.path.exists(current_path))


if __name__ == "__main__":
    unittest.main()
