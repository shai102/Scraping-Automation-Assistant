import logging
import os
import re
from datetime import datetime


LOG_KIND_APP = "app"
LOG_KIND_SCRAPE = "scrape"
LOG_KIND_METADATA = "metadata"
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def normalize_log_kind(kind: str) -> str:
    raw = str(kind or "").strip().lower()
    if raw == LOG_KIND_APP:
        return LOG_KIND_APP
    if raw == LOG_KIND_METADATA:
        return LOG_KIND_METADATA
    return LOG_KIND_SCRAPE


def resolve_log_dir(data_dir: str, kind: str) -> str:
    return os.path.join(str(data_dir or "").strip(), "logs", normalize_log_kind(kind))


def resolve_log_path(data_dir: str, kind: str, date_str: str | None = None) -> str:
    target_date = str(date_str or "").strip()
    if not _DATE_RE.match(target_date):
        target_date = datetime.now().strftime("%Y-%m-%d")
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
        if _DATE_RE.match(stem):
            dates.append(stem)
    return sorted(set(dates), reverse=True)


def _is_ignorable_connection_reset(record) -> bool:
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
            winerror == 10054
            or "WinError 10054" in text
            or "远程主机强迫关闭" in text
        )

    return (
        is_proactor_disconnect
        and "ConnectionResetError" in message
        and ("WinError 10054" in message or "远程主机强迫关闭" in message)
    )


class ErrorLogFilter(logging.Filter):
    def filter(self, record):
        return record.levelno >= logging.ERROR and not _is_ignorable_connection_reset(record)


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
        if record.levelno < logging.INFO:
            return False
        if _is_ignorable_connection_reset(record):
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
        if record.levelno < logging.INFO:
            return False
        if _is_ignorable_connection_reset(record):
            return False
        if record.name == "uvicorn.access":
            return False
        return not self._scrape_filter.filter(record) and not self._metadata_filter.filter(record)


class MetadataLogFilter(logging.Filter):
    _PREFIXES = (
        "monitor.watcher",
        "monitor.metadata_refresh",
        "core.services.worker_context",
    )
    _MESSAGE_MARKERS = (
        "元数据巡检:",
        "元数据巡检项:",
        "元数据巡检完成:",
        "元数据刷新:",
        "元数据刷新失败",
    )

    def filter(self, record):
        if record.levelno < logging.INFO:
            return False
        if _is_ignorable_connection_reset(record):
            return False
        if not any(str(record.name or "").startswith(prefix) for prefix in self._PREFIXES):
            return False
        message = str(record.getMessage() or "")
        return any(marker in message for marker in self._MESSAGE_MARKERS)


class DatePartitionedFileHandler(logging.Handler):
    terminator = "\n"

    def __init__(self, data_dir: str, kind: str, encoding: str = "utf-8"):
        super().__init__()
        self.data_dir = str(data_dir or "").strip()
        self.kind = normalize_log_kind(kind)
        self.encoding = encoding
        self._stream = None
        self._current_path = None
        self.createLock()

    def _path_for_record(self, record) -> str:
        date_str = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d")
        return resolve_log_path(self.data_dir, self.kind, date_str)

    def _reopen(self, target_path: str) -> None:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        if self._stream:
            try:
                self._stream.close()
            except Exception:
                pass
        self._stream = open(target_path, "a", encoding=self.encoding)
        self._current_path = target_path

    def emit(self, record):
        try:
            msg = self.format(record)
            target_path = self._path_for_record(record)
            self.acquire()
            try:
                if not self._stream or self._current_path != target_path:
                    self._reopen(target_path)
                self._stream.write(msg + self.terminator)
                self._stream.flush()
            finally:
                self.release()
        except Exception:
            self.handleError(record)

    def truncate_path(self, target_path: str) -> None:
        target_norm = os.path.normcase(os.path.normpath(target_path))
        self.acquire()
        try:
            if self._stream and self._current_path:
                current_norm = os.path.normcase(os.path.normpath(self._current_path))
                if current_norm == target_norm:
                    self._stream.seek(0)
                    self._stream.truncate(0)
                    self._stream.flush()
                    return
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with open(target_path, "w", encoding=self.encoding):
                pass
        finally:
            self.release()

    def close(self):
        self.acquire()
        try:
            if self._stream:
                try:
                    self._stream.close()
                except Exception:
                    pass
                self._stream = None
                self._current_path = None
        finally:
            self.release()
        super().close()


def setup_logging(data_dir: str, console_stream=None) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    app_handler = DatePartitionedFileHandler(data_dir, LOG_KIND_APP)
    app_handler.setLevel(logging.INFO)
    app_handler.addFilter(GeneralLogFilter())
    app_handler.setFormatter(fmt)
    root_logger.addHandler(app_handler)

    scrape_handler = DatePartitionedFileHandler(data_dir, LOG_KIND_SCRAPE)
    scrape_handler.setLevel(logging.INFO)
    scrape_handler.addFilter(ScrapeLogFilter())
    scrape_handler.setFormatter(fmt)
    root_logger.addHandler(scrape_handler)

    metadata_handler = DatePartitionedFileHandler(data_dir, LOG_KIND_METADATA)
    metadata_handler.setLevel(logging.INFO)
    metadata_handler.addFilter(MetadataLogFilter())
    metadata_handler.setFormatter(fmt)
    root_logger.addHandler(metadata_handler)

    if console_stream is not None:
        console_handler = logging.StreamHandler(console_stream)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(fmt)
        root_logger.addHandler(console_handler)
