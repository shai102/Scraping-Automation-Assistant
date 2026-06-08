"""Persistent task queue helpers for watcher work."""

from __future__ import annotations

import datetime
import logging
import os
from typing import Iterable

from sqlalchemy.exc import IntegrityError

from db.database import SessionLocal
from db.scrape_models import TaskQueue

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = ("queued", "running")
TERMINAL_STATUSES = ("done", "failed", "skipped")


def task_path_key(path: str) -> str:
    return os.path.normcase(os.path.normpath(str(path or "")))


def enqueue_task(
    db,
    path: str,
    *,
    folder_id: int | None = None,
    task_type: str = "scrape",
    source: str = "watchdog",
):
    """Create or reuse an active queue row for a path.

    Returns ``(task, created)``. Active tasks are de-duplicated by normalized path
    and task type; terminal rows remain as history.
    """
    norm_path = os.path.normpath(path)
    path_key = task_path_key(norm_path)
    task_type = task_type or "scrape"
    now = datetime.datetime.now()

    existing = _find_active_task(db, path_key, task_type)
    if existing:
        existing.path = norm_path
        existing.source = source or existing.source
        existing.updated_at = now
        if folder_id is not None and existing.folder_id is None:
            existing.folder_id = folder_id
        db.commit()
        return existing, False

    task = TaskQueue(
        path=norm_path,
        path_key=path_key,
        folder_id=folder_id,
        task_type=task_type,
        source=source or "watchdog",
        status="queued",
        attempts=0,
        created_at=now,
        updated_at=now,
    )
    db.add(task)
    try:
        db.commit()
        db.refresh(task)
        return task, True
    except IntegrityError:
        db.rollback()
        existing = _find_active_task(db, path_key, task_type)
        if existing:
            return existing, False
        raise


def _find_active_task(db, path_key: str, task_type: str):
    return (
        db.query(TaskQueue)
        .filter(
            TaskQueue.path_key == path_key,
            TaskQueue.task_type == task_type,
            TaskQueue.status.in_(ACTIVE_STATUSES),
        )
        .order_by(TaskQueue.id.desc())
        .first()
    )


def mark_task_running(db, task_id: int | None):
    if not task_id:
        return None
    task = db.query(TaskQueue).get(task_id)
    if not task:
        return None
    now = datetime.datetime.now()
    task.status = "running"
    task.attempts = int(task.attempts or 0) + 1
    task.started_at = now
    task.finished_at = None
    task.updated_at = now
    task.last_error = None
    db.commit()
    return task


def finish_task(db, task_id: int | None, status: str, error: str | None = None):
    if not task_id:
        return None
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"Unsupported task status: {status}")
    task = db.query(TaskQueue).get(task_id)
    if not task:
        return None
    now = datetime.datetime.now()
    task.status = status
    task.last_error = (error or "")[:1000] if error else None
    task.finished_at = now
    task.updated_at = now
    db.commit()
    return task


def finish_task_by_id(task_id: int | None, status: str, error: str | None = None):
    if not task_id:
        return None
    db = SessionLocal()
    try:
        return finish_task(db, task_id, status, error)
    finally:
        db.close()


def recover_stale_running_tasks(db, *, stale_minutes: int | None = 120) -> int:
    recover_all = stale_minutes is None or int(stale_minutes) <= 0
    cutoff = (
        datetime.datetime.now() - datetime.timedelta(minutes=max(1, int(stale_minutes)))
        if not recover_all
        else None
    )
    rows = db.query(TaskQueue).filter(TaskQueue.status == "running").all()
    count = 0
    now = datetime.datetime.now()
    for row in rows:
        last_seen = getattr(row, "updated_at", None) or getattr(row, "started_at", None)
        if not recover_all and last_seen and cutoff and last_seen > cutoff:
            continue
        row.status = "queued"
        row.started_at = None
        row.finished_at = None
        row.updated_at = now
        row.last_error = "程序上次处理被中断，已重新入队"
        count += 1
    if count:
        db.commit()
        logger.warning("Recovered running tasks: %s", count)
    return count


def load_queued_tasks(db, *, limit: int = 1000):
    return (
        db.query(TaskQueue)
        .filter(TaskQueue.status == "queued")
        .order_by(TaskQueue.created_at.asc(), TaskQueue.id.asc())
        .limit(limit)
        .all()
    )


def count_tasks_by_status(db, statuses: Iterable[str] = ACTIVE_STATUSES) -> dict[str, int]:
    result = {status: 0 for status in statuses}
    for status in statuses:
        result[status] = db.query(TaskQueue).filter(TaskQueue.status == status).count()
    return result
