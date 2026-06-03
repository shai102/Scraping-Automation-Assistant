import threading
import time

from utils.proxy import session


_tmdb_lock = threading.Lock()
_tmdb_tokens = 8.0
_tmdb_max_tokens = 8.0
_tmdb_refill_rate = 4.0
_tmdb_last_refill = time.monotonic()

_bgm_lock = threading.Lock()
_bgm_tokens = 5.0
_bgm_max_tokens = 5.0
_bgm_refill_rate = 5.0
_bgm_last_refill = time.monotonic()


def bgm_throttle():
    global _bgm_tokens, _bgm_last_refill
    while True:
        with _bgm_lock:
            now = time.monotonic()
            _bgm_tokens = min(
                _bgm_max_tokens,
                _bgm_tokens + (now - _bgm_last_refill) * _bgm_refill_rate,
            )
            _bgm_last_refill = now
            if _bgm_tokens >= 1.0:
                _bgm_tokens -= 1.0
                return
        time.sleep(0.15)


def tmdb_throttle():
    global _tmdb_tokens, _tmdb_last_refill
    while True:
        with _tmdb_lock:
            now = time.monotonic()
            elapsed = now - _tmdb_last_refill
            _tmdb_tokens = min(
                _tmdb_max_tokens,
                _tmdb_tokens + elapsed * _tmdb_refill_rate,
            )
            _tmdb_last_refill = now
            if _tmdb_tokens >= 1.0:
                _tmdb_tokens -= 1.0
                return
        time.sleep(0.15)


def tmdb_get(url, **kwargs):
    tmdb_throttle()
    return session.get(url, **kwargs)


def response_body_snippet(response, limit=300):
    if response is None:
        return ""
    try:
        body = response.text or ""
    except Exception:
        return ""
    compact = " ".join(str(body).split())
    if len(compact) > limit:
        return compact[:limit] + "..."
    return compact
