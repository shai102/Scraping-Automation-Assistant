import os
import re
from datetime import datetime


LOG_KIND_APP = "app"
LOG_KIND_SCRAPE = "scrape"
LOG_KIND_METADATA = "metadata"
LOG_DATE_LATEST = "latest"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
LATEST_DATE_VALUES = {LOG_DATE_LATEST, "__latest__"}


def normalize_log_kind(kind: str) -> str:
    raw = str(kind or "").strip().lower()
    if raw == LOG_KIND_APP:
        return LOG_KIND_APP
    if raw == LOG_KIND_METADATA:
        return LOG_KIND_METADATA
    return LOG_KIND_SCRAPE


def resolve_log_dir(data_dir: str, kind: str) -> str:
    return os.path.join(str(data_dir or "").strip(), "logs", normalize_log_kind(kind))


def resolve_log_date(data_dir: str, kind: str, date_str: str | None = None) -> str:
    target_date = str(date_str or "").strip()
    if target_date.lower() in LATEST_DATE_VALUES:
        dates = list_available_log_dates(data_dir, kind)
        if dates:
            return dates[0]
        return datetime.now().strftime("%Y-%m-%d")
    if DATE_RE.match(target_date):
        return target_date
    return datetime.now().strftime("%Y-%m-%d")


def resolve_log_path(data_dir: str, kind: str, date_str: str | None = None) -> str:
    target_date = resolve_log_date(data_dir, kind, date_str)
    return os.path.join(resolve_log_dir(data_dir, kind), f"{target_date}.log")


def list_available_log_dates(data_dir: str, kind: str) -> list[str]:
    log_dir = resolve_log_dir(data_dir, kind)
    if not os.path.isdir(log_dir):
        return []
    dates = []
    for entry in os.listdir(log_dir):
        if not entry.endswith(".log"):
            continue
        stem = entry[:-4]
        if DATE_RE.match(stem):
            dates.append(stem)
    return sorted(set(dates), reverse=True)
