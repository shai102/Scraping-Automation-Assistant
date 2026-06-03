import atexit
import json
import logging
import os
import sys
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta


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

_MOJIBAKE_SUSPECT_CHARS = set(
    "\u0447\u20ac\u50a8\u53a4\u546e\u589c\u5956\u59af\u5bf0\u612d\u6212\u6b13"
    "\u6ce6\u6ec5\u6fb6\u703d\u70ba\u72b5\u7459\u7487\u7730\u7966\u7ca8\u7f03"
    "\u8151\u89e6\u8fac\u934a\u9353\u9354\u935b\u9365\u93bc\u93c3\u93c8\u93cb"
    "\u9422\u950b\u95c4\u975b\ue048\uff46"
)
_MOJIBAKE_TEXT_REPLACEMENTS = {
    "\u935b\u6212\u8151": "命中",
    "\u9353\u0447\u6ce6": "剧集",
    "\u9422\u975b\u5956": "电影",
    "\u93bc\u6ec5\u50a8": "搜索",
    "\u6fb6\u8fac\u89e6": "失败",
    "\u93c8\ue048\u53a4\u7f03": "未配置",
    "\u7487\u950b\u7730": "请求",
    "\u7459\uff46\u703d": "解析",
    "\u9365\u70ba\u20ac": "回退",
    "\u934a\u6b13\u20ac": "候选",
    "\u59af\u2033\u7037": "模型",
    "\u9352\u3085\u757e": "判定",
    "\u9477\ue044\u59e9": "自动",
    "\u7487\u55d7\u57c6": "识别",
    "\u93c3\u72b5\u7ca8\u93cb": "无结果",
    "\u93c3\u72b3\u6665": "无效",
    "\u95c4\u612d\u7966": "限流",
    "\u5bf0\u546e\u589c\u9354": "待手动",
    "\u7f02\u64b3\u74e8": "缓存",
}


def _resolve_cache_file() -> str:
    if getattr(sys, "frozen", False):
        cfg_dir = os.path.dirname(sys.executable)
    else:
        cfg_dir = os.environ.get("DATA_DIR") or os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    return os.path.join(cfg_dir, "api_cache.json")


CACHE_FILE = _resolve_cache_file()


def _looks_like_mojibake(text):
    sample = str(text or "")
    if not sample:
        return False
    return any(ch in _MOJIBAKE_SUSPECT_CHARS for ch in sample)


def _score_human_readable_text(text):
    sample = str(text or "")
    if not sample:
        return 0
    score = 0
    for ch in sample:
        code = ord(ch)
        if 0x4E00 <= code <= 0x9FFF:
            score += 3
        elif ch.isascii() and (ch.isalnum() or ch in " -_:/.,()[]{}+&!?"):
            score += 1
        elif ch in "\r\n\t":
            score += 0
        else:
            score -= 1
    return score


def _repair_mojibake_text(text):
    sample = str(text or "")
    if not _looks_like_mojibake(sample):
        return sample
    replaced = sample
    for old, new in _MOJIBAKE_TEXT_REPLACEMENTS.items():
        replaced = replaced.replace(old, new)
    if replaced != sample and not _looks_like_mojibake(replaced):
        return replaced
    try:
        repaired = sample.encode("gbk", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return replaced
    if not repaired or repaired == sample:
        return replaced
    if _score_human_readable_text(repaired) < _score_human_readable_text(sample):
        return replaced
    return repaired


def _repair_legacy_cache_strings(value):
    if isinstance(value, str):
        return _repair_mojibake_text(value)
    if isinstance(value, list):
        return [_repair_legacy_cache_strings(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_repair_legacy_cache_strings(v) for v in value)
    if isinstance(value, dict):
        return {
            _repair_legacy_cache_strings(k): _repair_legacy_cache_strings(v)
            for k, v in value.items()
        }
    return value


def _load_cache_from_disk():
    if not os.path.exists(CACHE_FILE):
        return {}

    try:
        for encoding in ["utf-8", "gbk", "latin-1"]:
            try:
                with open(CACHE_FILE, "r", encoding=encoding) as fh:
                    cache = json.load(fh)
                break
            except UnicodeDecodeError:
                continue
        else:
            with open(CACHE_FILE, "rb") as fh:
                content = fh.read().decode("utf-8", errors="ignore")
            cache = json.loads(content)
        if not isinstance(cache, dict):
            return {}
        return _repair_legacy_cache_strings(cache)
    except Exception as err:
        logging.error(f"加载缓存失败: {err}")
        return {}


def _prune_expired_cache_entries(cache, now_ts=None):
    if _cache_expiry_days == 0:
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
    global _cache_data
    if _cache_data is None:
        _cache_data = _load_cache_from_disk()


def save_cache(cache):
    temp_file = CACHE_FILE + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as fh:
            json.dump(cache, fh, indent=2, ensure_ascii=False)

        import shutil

        shutil.move(temp_file, CACHE_FILE)
    except Exception as err:
        logging.error(f"保存缓存失败: {err}")
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception:
            pass


def _flush_cache_to_disk_unlocked(force=False):
    global _cache_dirty, _cache_last_flush_ts, _cache_write_count
    if not _cache_dirty:
        return False

    now_ts = datetime.now().timestamp()
    should_flush = force
    if not should_flush:
        should_flush = (
            _cache_write_count >= CACHE_FLUSH_MAX_WRITES
            or now_ts - _cache_last_flush_ts >= CACHE_FLUSH_INTERVAL_SECONDS
        )
    if not should_flush:
        return False

    save_cache(_cache_data or {})
    _cache_dirty = False
    _cache_write_count = 0
    _cache_last_flush_ts = now_ts
    return True


def load_cache():
    with _cache_file_lock:
        _ensure_cache_loaded_unlocked()
        _prune_expired_cache_entries(_cache_data)
        return dict(_cache_data)


def set_cache_expiry_days(days: int):
    global _cache_expiry_days
    _cache_expiry_days = max(0, int(days))


def clear_api_cache_file():
    global _cache_data, _cache_dirty, _cache_write_count, _cache_last_flush_ts
    with _cache_file_lock:
        try:
            _cache_data = {}
            _cache_dirty = False
            _cache_write_count = 0
            _cache_last_flush_ts = 0.0
            if os.path.exists(CACHE_FILE):
                os.remove(CACHE_FILE)
            return True
        except Exception as err:
            logging.error(f"清理API缓存文件失败: {err}")
            return False


def get_cache_key(api_name, query):
    return f"{api_name}:{str(query)}"


def invalidate_cache_prefix(prefix):
    global _cache_dirty
    with _cache_file_lock:
        _ensure_cache_loaded_unlocked()
        keys = [key for key in list(_cache_data.keys()) if key.startswith(prefix)]
        for key in keys:
            del _cache_data[key]
        if keys:
            _cache_dirty = True
            _flush_cache_to_disk_unlocked(force=True)


@contextmanager
def bypass_api_cache(enabled=True):
    token = _api_cache_bypass.set(bool(enabled))
    try:
        yield
    finally:
        _api_cache_bypass.reset(token)


def cached_request(api_func, cache_key, *args, **kwargs):
    global _cache_dirty, _cache_write_count
    if _api_cache_bypass.get():
        return api_func(*args, **kwargs)

    now_ts = datetime.now().timestamp()

    with _cache_file_lock:
        _ensure_cache_loaded_unlocked()
        expired_count = _prune_expired_cache_entries(_cache_data, now_ts)
        if expired_count > 0:
            _cache_dirty = True
            _flush_cache_to_disk_unlocked(force=False)

        cached_entry = (_cache_data or {}).get(cache_key)
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
        with _cache_file_lock:
            _ensure_cache_loaded_unlocked()
            _cache_data[cache_key] = {
                "data": result,
                "expiry": (
                    datetime.now()
                    + timedelta(days=_cache_expiry_days if _cache_expiry_days > 0 else 36500)
                ).timestamp(),
            }
            _cache_dirty = True
            _cache_write_count += 1
            _flush_cache_to_disk_unlocked(force=False)

    return result


def flush_api_cache(force=False):
    with _cache_file_lock:
        _ensure_cache_loaded_unlocked()
        return _flush_cache_to_disk_unlocked(force=force)


atexit.register(lambda: flush_api_cache(force=True))
