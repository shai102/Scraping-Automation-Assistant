import atexit
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta

from utils import cache_runtime as runtime
from utils.cache_repair import repair_legacy_cache_strings


def _load_cache_from_disk():
    if not os.path.exists(runtime.CACHE_FILE):
        return {}

    try:
        for encoding in ["utf-8", "gbk", "latin-1"]:
            try:
                with open(runtime.CACHE_FILE, "r", encoding=encoding) as fh:
                    cache = json.load(fh)
                break
            except UnicodeDecodeError:
                continue
        else:
            with open(runtime.CACHE_FILE, "rb") as fh:
                content = fh.read().decode("utf-8", errors="ignore")
            cache = json.loads(content)
        if not isinstance(cache, dict):
            return {}
        return repair_legacy_cache_strings(cache)
    except Exception as err:
        logging.error(f"加载缓存失败: {err}")
        return {}


def _prune_expired_cache_entries(cache, now_ts=None):
    if runtime._cache_expiry_days == 0:
        return 0
    now_value = now_ts or datetime.now().timestamp()
    expired_keys = [
        key
        for key, value in list((cache or {}).items())
        if not isinstance(value, dict) or value.get("expiry", 0) < now_value
    ]
    for key in expired_keys:
        cache.pop(key, None)
    return len(expired_keys)


def _ensure_cache_loaded_unlocked():
    if runtime._cache_data is None:
        runtime._cache_data = _load_cache_from_disk()


def save_cache(cache):
    temp_file = runtime.CACHE_FILE + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as fh:
            json.dump(cache, fh, indent=2, ensure_ascii=False)

        import shutil

        shutil.move(temp_file, runtime.CACHE_FILE)
    except Exception as err:
        logging.error(f"保存缓存失败: {err}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass


def _flush_cache_to_disk_unlocked(force=False):
    if not runtime._cache_dirty:
        return False

    now_ts = datetime.now().timestamp()
    should_flush = force
    if not should_flush:
        should_flush = (
            runtime._cache_write_count >= runtime.CACHE_FLUSH_MAX_WRITES
            or now_ts - runtime._cache_last_flush_ts >= runtime.CACHE_FLUSH_INTERVAL_SECONDS
        )
    if not should_flush:
        return False

    save_cache(runtime._cache_data or {})
    runtime._cache_dirty = False
    runtime._cache_write_count = 0
    runtime._cache_last_flush_ts = now_ts
    return True


def load_cache():
    with runtime._cache_file_lock:
        _ensure_cache_loaded_unlocked()
        _prune_expired_cache_entries(runtime._cache_data)
        return dict(runtime._cache_data)


def set_cache_expiry_days(days: int):
    runtime._cache_expiry_days = max(0, int(days))


def clear_api_cache_file():
    with runtime._cache_file_lock:
        try:
            runtime._cache_data = {}
            runtime._cache_dirty = False
            runtime._cache_write_count = 0
            runtime._cache_last_flush_ts = 0.0
            if os.path.exists(runtime.CACHE_FILE):
                os.remove(runtime.CACHE_FILE)
            return True
        except Exception as err:
            logging.error(f"清理API缓存文件失败: {err}")
            return False


def get_cache_key(api_name, query):
    return f"{api_name}:{str(query)}"


def invalidate_cache_prefix(prefix):
    with runtime._cache_file_lock:
        _ensure_cache_loaded_unlocked()
        keys = [key for key in list(runtime._cache_data.keys()) if key.startswith(prefix)]
        for key in keys:
            del runtime._cache_data[key]
        if keys:
            runtime._cache_dirty = True
            _flush_cache_to_disk_unlocked(force=True)


@contextmanager
def bypass_api_cache(enabled=True):
    token = runtime._api_cache_bypass.set(bool(enabled))
    try:
        yield
    finally:
        runtime._api_cache_bypass.reset(token)


def cached_request(api_func, cache_key, *args, **kwargs):
    if runtime._api_cache_bypass.get():
        return api_func(*args, **kwargs)

    now_ts = datetime.now().timestamp()
    with runtime._cache_file_lock:
        _ensure_cache_loaded_unlocked()
        expired_count = _prune_expired_cache_entries(runtime._cache_data, now_ts)
        if expired_count > 0:
            runtime._cache_dirty = True
            _flush_cache_to_disk_unlocked(force=False)

        cached_entry = (runtime._cache_data or {}).get(cache_key)
        if isinstance(cached_entry, dict) and cached_entry.get("expiry", 0) >= now_ts:
            return cached_entry.get("data")

    result = api_func(*args, **kwargs)

    is_valid = True
    if result is None:
        is_valid = False
    elif isinstance(result, (list, dict, set)) and len(result) == 0:
        is_valid = False
    elif isinstance(result, str) and not result.strip():
        is_valid = False
    elif isinstance(result, tuple):
        if len(result) >= 2 and result[1] == "None":
            is_valid = False
        elif len(result) >= 3 and not result[0] and not result[1]:
            is_valid = False

    if is_valid:
        with runtime._cache_file_lock:
            _ensure_cache_loaded_unlocked()
            runtime._cache_data[cache_key] = {
                "data": result,
                "expiry": (
                    datetime.now()
                    + timedelta(days=runtime._cache_expiry_days if runtime._cache_expiry_days > 0 else 36500)
                ).timestamp(),
            }
            runtime._cache_dirty = True
            runtime._cache_write_count += 1
            _flush_cache_to_disk_unlocked(force=False)

    return result


def flush_api_cache(force=False):
    with runtime._cache_file_lock:
        _ensure_cache_loaded_unlocked()
        return _flush_cache_to_disk_unlocked(force=force)


atexit.register(lambda: flush_api_cache(force=True))
