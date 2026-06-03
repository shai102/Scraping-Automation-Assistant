import os
import re
import sys
from collections import deque

from core.logging.annotation import analyze_log_message
from utils.logging_setup import list_available_log_dates, normalize_log_kind, resolve_log_path


_LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (?P<level>[A-Z]+) - (?P<message>.*)$"
)
_METADATA_PREFIXES = (
    "元数据巡检:",
    "元数据巡检项:",
    "元数据巡检完成:",
    "元数据刷新:",
    "元数据刷新失败:",
)


def base_data_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.environ.get("DATA_DIR") or os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )


def tail_lines(path: str, max_lines: int) -> list[str]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return list(deque(fh, maxlen=max_lines))


def is_metadata_message(message: str) -> bool:
    text = str(message or "")
    return any(text.startswith(prefix) for prefix in _METADATA_PREFIXES)


def parse_log_entry(line: str) -> dict:
    match = _LOG_LINE_RE.match(line)
    if match:
        return {
            "timestamp": match.group("ts"),
            "level": match.group("level"),
            "message": match.group("message"),
            "raw": line,
        }
    return {
        "timestamp": "",
        "level": "INFO",
        "message": line,
        "raw": line,
    }


def read_log_items(
    *,
    data_dir: str,
    kind: str,
    limit: int,
    level: str = "",
    keyword: str | None = None,
    date: str | None = None,
) -> dict:
    log_kind = normalize_log_kind(kind)
    log_path = resolve_log_path(data_dir, log_kind, date)
    raw_lines = tail_lines(log_path, max(limit * 10, 500))
    want_level = str(level or "").strip().upper()
    want_keyword = str(keyword or "").strip().lower()
    available_dates = list_available_log_dates(data_dir, log_kind)
    selected_date = os.path.splitext(os.path.basename(log_path))[0]

    items = []
    for raw in reversed(raw_lines):
        line = raw.rstrip("\r\n")
        if not line:
            continue

        entry = parse_log_entry(line)
        if log_kind == "scrape" and is_metadata_message(entry["message"]):
            continue
        if want_level and entry["level"] != want_level:
            continue
        if want_keyword and want_keyword not in entry["raw"].lower():
            continue

        analyzed = analyze_log_message(entry["message"])
        entry["kind"] = analyzed.get("kind", "general")
        entry["parsed"] = analyzed.get("parsed", {})
        entry["annotation"] = analyzed.get("annotation")
        items.append(entry)
        if len(items) >= limit:
            break

    return {
        "path": log_path,
        "exists": os.path.isfile(log_path),
        "kind": log_kind,
        "selected_date": selected_date,
        "available_dates": available_dates,
        "items": items,
    }
