"""Periodic cleanup for bounded runtime data growth."""

import datetime
import logging
import os
import time

from monitor.task_queue import cleanup_terminal_tasks
from core.services.archive_journal import cleanup_archive_operations
from utils.app_runtime import DATA_DIR
from utils.logging_paths import LOG_KIND_APP, LOG_KIND_METADATA, LOG_KIND_SCRAPE, resolve_log_dir

logger = logging.getLogger(__name__)


def cleanup_old_logs(*, retention_days: int = 30, data_dir: str = DATA_DIR) -> int:
    cutoff = datetime.datetime.now() - datetime.timedelta(days=max(1, int(retention_days or 30)))
    removed = 0
    for kind in (LOG_KIND_APP, LOG_KIND_SCRAPE, LOG_KIND_METADATA):
        log_dir = resolve_log_dir(data_dir, kind)
        if not os.path.isdir(log_dir):
            continue
        for entry in os.listdir(log_dir):
            if not entry.endswith(".log"):
                continue
            try:
                file_date = datetime.datetime.strptime(entry[:-4], "%Y-%m-%d")
            except ValueError:
                continue
            if file_date < cutoff:
                try:
                    os.remove(os.path.join(log_dir, entry))
                    removed += 1
                except OSError as err:
                    logger.warning("清理过期日志失败 %s: %s", entry, err)
    return removed


def run_maintenance_once(watcher) -> dict:
    cfg = getattr(getattr(watcher, "_worker_ctx", None), "_cfg", {}) or {}
    db = watcher._session_factory()
    try:
        tasks = cleanup_terminal_tasks(db, retention_days=cfg.get("task_retention_days", 30))
        archive_operations = cleanup_archive_operations(
            db, retention_days=cfg.get("task_retention_days", 30)
        )
    finally:
        db.close()
    logs = cleanup_old_logs(retention_days=cfg.get("log_retention_days", 30))
    watcher._last_maintenance_at = datetime.datetime.now()
    if tasks or logs or archive_operations:
        logger.info(
            "运行数据清理完成: tasks=%s archive_operations=%s logs=%s",
            tasks,
            archive_operations,
            logs,
        )
    return {"tasks": tasks, "archive_operations": archive_operations, "logs": logs}


def maintenance_loop(watcher, interval_seconds: int = 21600):
    while watcher._running:
        try:
            run_maintenance_once(watcher)
        except Exception as err:
            logger.error("运行数据清理失败: %s", err)
        for _ in range(max(1, int(interval_seconds // 5))):
            if not watcher._running:
                return
            time.sleep(5)
