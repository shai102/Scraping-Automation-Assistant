import datetime
import unittest

from monitor.task_queue import recover_stale_running_tasks


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


class FakeTask:
    def __init__(self, updated_at):
        self.status = "running"
        self.started_at = updated_at
        self.updated_at = updated_at
        self.finished_at = None
        self.last_error = None


class TaskQueueRecoveryTests(unittest.TestCase):
    def test_startup_recovery_requeues_all_running_tasks(self):
        fresh = FakeTask(datetime.datetime.now())
        old = FakeTask(datetime.datetime.now() - datetime.timedelta(hours=3))
        session = FakeSession([fresh, old])

        count = recover_stale_running_tasks(session, stale_minutes=0)

        self.assertEqual(2, count)
        self.assertEqual("queued", fresh.status)
        self.assertEqual("queued", old.status)
        self.assertIsNone(fresh.started_at)
        self.assertIsNone(old.finished_at)
        self.assertIn("重新入队", fresh.last_error)
        self.assertEqual(1, session.commits)


if __name__ == "__main__":
    unittest.main()
