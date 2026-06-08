import json
import os
import subprocess
import sys
import tempfile
import unittest


class DatabaseRuntimeTests(unittest.TestCase):
    def test_init_db_sets_pragmas_and_indexes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env = dict(os.environ)
            env["DATA_DIR"] = tmpdir
            script = """
import json
import db.database as database

database.init_db()
with database.engine.connect() as conn:
    payload = {
        "journal_mode": conn.exec_driver_sql("PRAGMA journal_mode").scalar(),
        "busy_timeout": conn.exec_driver_sql("PRAGMA busy_timeout").scalar(),
        "indexes": [
            row[1]
            for row in conn.exec_driver_sql("PRAGMA index_list('scrape_records')").fetchall()
        ],
    }
print(json.dumps(payload))
"""
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                env=env,
                text=True,
                capture_output=True,
                check=True,
            )
            payload = json.loads(result.stdout)

            self.assertEqual("wal", str(payload["journal_mode"]).lower())
            self.assertEqual(30000, payload["busy_timeout"])
            self.assertIn("idx_scrape_records_original_path", payload["indexes"])
            self.assertIn("idx_scrape_records_status", payload["indexes"])


if __name__ == "__main__":
    unittest.main()
