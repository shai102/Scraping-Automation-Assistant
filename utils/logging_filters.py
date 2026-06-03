import logging


def is_ignorable_connection_reset(record) -> bool:
    message = str(record.getMessage() or "")
    exc = record.exc_info[1] if record.exc_info else None
    is_proactor_disconnect = (
        "_ProactorBasePipeTransport._call_connection_lost" in message
        or "_call_connection_lost" in message
        or record.name == "asyncio"
    )

    if isinstance(exc, ConnectionResetError):
        winerror = getattr(exc, "winerror", None)
        text = str(exc)
        return is_proactor_disconnect and (
            winerror == 10054 or "WinError 10054" in text or "远程主机强迫关闭" in text
        )

    return (
        is_proactor_disconnect
        and "ConnectionResetError" in message
        and ("WinError 10054" in message or "远程主机强迫关闭" in message)
    )


class ErrorLogFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.ERROR and not is_ignorable_connection_reset(record)


class ScrapeLogFilter(logging.Filter):
    _PREFIXES = (
        "monitor.watcher",
        "api.routes.records",
        "core.workers.task_runner",
        "core.workers.execution_runner",
        "core.services.worker_context",
    )
    _MESSAGE_MARKERS = (
        "开始识别:",
        "识别完成:",
        "识别失败:",
        "资料库匹配:",
        "资料库匹配失败:",
        "NFO fast-path:",
        "Archived:",
        "Archive failed",
        "Recognition failed",
        "检测到缺失的刮削产物",
        "跳过已有元数据",
        "恢复:",
        "Restored:",
    )

    def filter(self, record):
        if record.levelno < logging.INFO or is_ignorable_connection_reset(record):
            return False
        if not any(str(record.name or "").startswith(prefix) for prefix in self._PREFIXES):
            return False
        message = str(record.getMessage() or "")
        return any(marker in message for marker in self._MESSAGE_MARKERS)


class MetadataLogFilter(logging.Filter):
    _PREFIXES = ("monitor.watcher", "monitor.metadata_refresh", "core.services.worker_context")
    _MESSAGE_MARKERS = (
        "元数据巡检:",
        "元数据巡检项:",
        "元数据巡检完成:",
        "元数据刷新:",
        "元数据刷新失败",
    )

    def filter(self, record):
        if record.levelno < logging.INFO or is_ignorable_connection_reset(record):
            return False
        if not any(str(record.name or "").startswith(prefix) for prefix in self._PREFIXES):
            return False
        message = str(record.getMessage() or "")
        return any(marker in message for marker in self._MESSAGE_MARKERS)


class GeneralLogFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self._scrape_filter = ScrapeLogFilter()
        self._metadata_filter = MetadataLogFilter()

    def filter(self, record):
        if record.levelno < logging.INFO or is_ignorable_connection_reset(record):
            return False
        if record.name == "uvicorn.access":
            return False
        return not self._scrape_filter.filter(record) and not self._metadata_filter.filter(record)
