"""Retry classification and delayed persistent requeue helpers."""

import errno
import threading

from monitor.task_queue import requeue_task_by_id


RETRYABLE_MARKERS = (
    "rate limit", "rate_limited", "限流", "too many requests", "429",
    "timeout", "timed out", "超时", "connection reset", "connection aborted",
    "temporarily unavailable", "temporary failure", "网络连接", "文件仍在写入",
)


def classify_retryable_error(error) -> tuple[bool, str]:
    if isinstance(error, PermissionError):
        return False, "permission"
    if isinstance(error, OSError) and getattr(error, "errno", None) in {
        errno.EACCES, errno.EPERM, errno.ENOSPC, errno.EXDEV,
    }:
        return False, "filesystem"
    text = str(error or "").lower()
    if any(marker in text for marker in RETRYABLE_MARKERS):
        return True, "transient"
    return False, "permanent"


def retry_delay_seconds(attempts: int, base_seconds: int = 30, max_seconds: int = 1800) -> int:
    exponent = max(0, min(6, int(attempts or 1) - 1))
    return min(max_seconds, max(1, int(base_seconds)) * (2 ** exponent))


def can_retry(attempts: int, max_attempts: int = 5) -> bool:
    return int(attempts or 0) < max(1, int(max_attempts or 5))


def schedule_task_retry(watcher, path: str, task_id: int | None, reason: str, delay: int) -> bool:
    if not task_id:
        return False
    task = requeue_task_by_id(task_id, reason)
    if not task:
        return False

    def _enqueue():
        if getattr(watcher, "_running", False) and path:
            watcher.enqueue(path, source="auto_retry", immediate=True, force=True)

    timer = threading.Timer(max(1, int(delay)), _enqueue)
    timer.daemon = True
    timer.start()
    return True
