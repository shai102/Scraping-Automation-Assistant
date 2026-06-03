import logging
import time

from monitor.metadata_refresh import run_metadata_refresh_pass, refresh_record_metadata as refresh_record_metadata_impl


logger = logging.getLogger(__name__)


def metadata_refresh_loop(
    watcher,
    *,
    default_interval_hours: int,
    default_lookback_days: int,
):
    time.sleep(60)
    while watcher._running:
        cfg = watcher._worker_ctx._cfg if watcher._worker_ctx else {}
        enabled = cfg.get("metadata_refresh_enabled", True)
        interval_hours = cfg.get("metadata_refresh_interval_hours", default_interval_hours)
        interval_seconds = max(1800, interval_hours * 3600)

        if enabled:
            try:
                refresh_incomplete_records(watcher, default_lookback_days=default_lookback_days)
            except Exception as err:
                logger.error(f"Metadata refresh error: {err}")

        slept = 0.0
        while slept < interval_seconds and watcher._running:
            chunk = min(30.0, interval_seconds - slept)
            time.sleep(chunk)
            slept += chunk


def refresh_incomplete_records(watcher, *, default_lookback_days: int):
    if not watcher._worker_ctx:
        return
    cfg = watcher._worker_ctx._cfg if watcher._worker_ctx else {}
    lookback_days = cfg.get("metadata_refresh_lookback_days", default_lookback_days)
    run_metadata_refresh_pass(
        watcher._worker_ctx,
        lookback_days=lookback_days,
        running_check=lambda: watcher._running,
        broadcast_fn=watcher._broadcast,
    )


def refresh_single_record(watcher, record, db) -> bool:
    return refresh_record_metadata_impl(record, db, watcher._worker_ctx, watcher._broadcast)
