import datetime
import unittest

from monitor.record_state import reset_stale_processing_records


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return self.rows


class FakeSession:
    def __init__(self, rows):
        self.rows = rows
        self.commits = 0

    def query(self, *args, **kwargs):
        return FakeQuery(self.rows)

    def commit(self):
        self.commits += 1


class FakeRecord:
    def __init__(self, updated_at):
        self.status = "processing"
        self.updated_at = updated_at
        self.created_at = updated_at
        self.error_msg = None


class RecordStateTests(unittest.TestCase):
    def test_reset_stale_processing_records_only_marks_old_rows(self):
        old = FakeRecord(datetime.datetime.now() - datetime.timedelta(hours=3))
        fresh = FakeRecord(datetime.datetime.now())
        session = FakeSession([old, fresh])

        count = reset_stale_processing_records(session, stale_minutes=120)

        self.assertEqual(1, count)
        self.assertEqual("failed", old.status)
        self.assertIn("可手动重试", old.error_msg)
        self.assertEqual("processing", fresh.status)
        self.assertEqual(1, session.commits)


if __name__ == "__main__":
    unittest.main()
