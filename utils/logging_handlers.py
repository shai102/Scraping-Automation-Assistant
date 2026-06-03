import logging
import os
from datetime import datetime

from utils.logging_paths import normalize_log_kind, resolve_log_path


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
