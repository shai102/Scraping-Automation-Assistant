import os
import sys
import threading
from contextvars import ContextVar


CACHE_EXPIRY_DAYS = 7
CACHE_FLUSH_INTERVAL_SECONDS = 8
CACHE_FLUSH_MAX_WRITES = 20

_cache_expiry_days = CACHE_EXPIRY_DAYS
_cache_file_lock = threading.Lock()
_cache_data = None
_cache_dirty = False
_cache_write_count = 0
_cache_last_flush_ts = 0.0
_api_cache_bypass = ContextVar("api_cache_bypass", default=False)


def resolve_cache_file() -> str:
    if getattr(sys, "frozen", False):
        cfg_dir = os.path.dirname(sys.executable)
    else:
        cfg_dir = os.environ.get("DATA_DIR") or os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    return os.path.join(cfg_dir, "api_cache.json")


CACHE_FILE = resolve_cache_file()
