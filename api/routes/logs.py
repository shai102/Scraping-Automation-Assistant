import logging
import os
from typing import Optional

from fastapi import APIRouter, Query

from core.logging.reader import base_data_dir, read_log_items
from utils.logging_setup import DatePartitionedFileHandler, normalize_log_kind, resolve_log_path


router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("")
def read_logs(
    limit: int = Query(200, ge=20, le=1000),
    level: str = Query("", description="INFO / WARNING / ERROR"),
    keyword: Optional[str] = Query(None),
    kind: str = Query("scrape", description="scrape / app / metadata"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD 或 latest"),
):
    return read_log_items(
        data_dir=base_data_dir(),
        kind=kind,
        limit=limit,
        level=level,
        keyword=keyword,
        date=date,
    )


@router.delete("")
def clear_logs(
    kind: str = Query("scrape", description="scrape / app / metadata"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD 或 latest"),
):
    data_dir = base_data_dir()
    log_kind = normalize_log_kind(kind)
    log_path = resolve_log_path(data_dir, log_kind, date)
    if not os.path.isfile(log_path):
        return {"ok": True, "message": "日志文件不存在，无需清除"}
    try:
        norm = os.path.normcase(os.path.normpath(log_path))
        truncated = False
        for logger_name in list(logging.Logger.manager.loggerDict) + [None]:
            log_obj = logging.getLogger(logger_name)
            for handler in list(getattr(log_obj, "handlers", [])):
                if isinstance(handler, DatePartitionedFileHandler):
                    handler.truncate_path(log_path)
                    truncated = True
                elif isinstance(handler, logging.FileHandler):
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
                        handler.stream.seek(0)
                        handler.stream.truncate(0)
                        handler.stream.flush()
                        truncated = True
        if not truncated:
            with open(log_path, "w", encoding="utf-8") as fh:
                fh.truncate(0)
        return {"ok": True, "message": "日志已清除"}
    except Exception as err:
        return {"ok": False, "message": f"清除失败: {err}"}
