import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.logging.reader import read_log_items
from utils.logging_paths import LOG_DATE_LATEST, resolve_log_path


def _write_log(data_dir, kind, date, message):
    path = resolve_log_path(str(data_dir), kind, date)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"{date} 12:00:00,000 - INFO - {message}\n")
    return path


def test_resolve_latest_log_path_uses_newest_available_date(tmp_path):
    _write_log(tmp_path, "scrape", "2026-07-01", "old")
    newest = _write_log(tmp_path, "scrape", "2026-07-03", "new")

    assert resolve_log_path(str(tmp_path), "scrape", LOG_DATE_LATEST) == newest


def test_read_log_items_latest_date_reads_newest_log(tmp_path):
    _write_log(tmp_path, "scrape", "2026-07-01", "old message")
    _write_log(tmp_path, "scrape", "2026-07-03", "new message")

    payload = read_log_items(
        data_dir=str(tmp_path),
        kind="scrape",
        limit=10,
        date=LOG_DATE_LATEST,
    )

    assert payload["selected_date"] == "2026-07-03"
    assert payload["available_dates"] == ["2026-07-03", "2026-07-01"]
    assert [item["message"] for item in payload["items"]] == ["new message"]
