import logging
import os
import re
import sys
from collections import deque
from typing import Optional

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/logs", tags=["logs"])

_LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (?P<level>[A-Z]+) - (?P<message>.*)$"
)


def _resolve_log_path(kind: str = "app") -> str:
    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
    filename = "scrape_process.log" if str(kind or "").strip().lower() == "scrape" else "media_renamer.log"
    return os.path.join(base_dir, filename)


def _tail_lines(path: str, max_lines: int) -> list[str]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return list(deque(fh, maxlen=max_lines))


@router.get("")
def read_logs(
    limit: int = Query(200, ge=20, le=1000),
    level: str = Query("", description="INFO / WARNING / ERROR"),
    keyword: Optional[str] = Query(None),
    kind: str = Query("scrape", description="scrape / app"),
):
    log_path = _resolve_log_path(kind)
    raw_lines = _tail_lines(log_path, max(limit * 10, 500))
    want_level = str(level or "").strip().upper()
    want_keyword = str(keyword or "").strip().lower()

    items = []
    for raw in reversed(raw_lines):
        line = raw.rstrip("\r\n")
        if not line:
            continue

        match = _LOG_LINE_RE.match(line)
        if match:
            entry = {
                "timestamp": match.group("ts"),
                "level": match.group("level"),
                "message": match.group("message"),
                "raw": line,
            }
        else:
            entry = {
                "timestamp": "",
                "level": "INFO",
                "message": line,
                "raw": line,
            }

        if want_level and entry["level"] != want_level:
            continue
        if want_keyword and want_keyword not in entry["raw"].lower():
            continue

        items.append(entry)
        if len(items) >= limit:
            break

    return {
        "path": log_path,
        "exists": os.path.isfile(log_path),
        "items": items,
    }


@router.delete("")
def clear_logs(kind: str = Query("scrape", description="scrape / app")):
    log_path = _resolve_log_path(kind)
    if not os.path.isfile(log_path):
        return {"ok": True, "message": "日志文件不存在，无需清除"}
    try:
        # Reset all FileHandler streams pointing to this log file so their
        # write offset doesn't cause NUL-byte padding after truncation.
        norm = os.path.normcase(os.path.normpath(log_path))
        for logger_name in list(logging.Logger.manager.loggerDict) + [None]:
            log_obj = logging.getLogger(logger_name)
            for handler in list(getattr(log_obj, "handlers", [])):
                if isinstance(handler, logging.FileHandler):
                    try:
                        handler_path = os.path.normcase(
                            os.path.normpath(handler.baseFilename)
                        )
                    except Exception:
                        continue
                    if handler_path == norm:
                        handler.close()
                        handler.stream = open(
                            handler.baseFilename,
                            handler.mode,
                            encoding=handler.encoding,
                        )
        # Now truncate the file
        with open(log_path, "w", encoding="utf-8") as fh:
            fh.truncate(0)
        return {"ok": True, "message": "日志已清除"}
    except Exception as err:
        return {"ok": False, "message": f"清除失败: {err}"}
