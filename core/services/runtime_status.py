"""Read-only runtime status snapshot shared by health and UI endpoints."""

from db.scrape_models import ArchiveOperation, MonitorFolder, ScrapeRecord, TaskQueue


def build_runtime_status(db, watcher=None, websocket_clients: int = 0) -> dict:
    status = {
        "ok": bool(watcher and watcher._running),
        "database": "ok",
        "watcher": "running" if watcher and watcher._running else "stopped",
        "active_folders": db.query(MonitorFolder).filter(MonitorFolder.enabled.is_(True)).count(),
        "tasks": {},
        "records": {},
        "archive_operations": {},
        "pending_debounce": len(getattr(watcher, "_pending", {}) or {}),
        "active_directories": len(getattr(watcher, "_active_dirs", set()) or set()),
        "websocket_clients": int(websocket_clients or 0),
        "started_at": getattr(watcher, "_started_at", None),
        "last_poll_at": getattr(watcher, "_last_poll_at", None),
        "last_success_at": getattr(watcher, "_last_success_at", None),
        "last_maintenance_at": getattr(watcher, "_last_maintenance_at", None),
    }
    for value in ("queued", "running", "failed", "done"):
        status["tasks"][value] = db.query(TaskQueue).filter(TaskQueue.status == value).count()
    for value in ("processing", "success", "pending_manual", "failed"):
        status["records"][value] = db.query(ScrapeRecord).filter(ScrapeRecord.status == value).count()
    for value in ("running", "completed", "failed", "recovered"):
        status["archive_operations"][value] = (
            db.query(ArchiveOperation).filter(ArchiveOperation.status == value).count()
        )
    return status
