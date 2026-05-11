"""Emby / Jellyfin library refresh notification.

After a configurable quiet period, triggers a library scan on the
configured Emby/Jellyfin server so newly scraped content appears
without manual intervention.

Both Emby and Jellyfin expose the same  POST /Library/Refresh  endpoint,
so this module works with either media server.
"""

import logging
import threading
from typing import Callable

from utils.helpers import request_get, request_post

logger = logging.getLogger(__name__)


def _auth_headers(api_key: str) -> dict:
    return {
        "X-Emby-Token": api_key,
        "X-MediaBrowser-Token": api_key,   # Jellyfin compat alias
        "Content-Type": "application/json",
    }


def trigger_library_refresh(base_url: str, api_key: str) -> tuple[bool, str]:
    """POST /Library/Refresh to start a full library scan.

    Returns (ok, message).
    """
    base = base_url.rstrip("/")
    try:
        resp = request_post(
            f"{base}/Library/Refresh",
            headers=_auth_headers(api_key),
            timeout=(5, 20),
        )
        if resp.status_code in (200, 204):
            return True, "扫描任务已触发"
        return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as e:
        return False, str(e)[:200]


def test_emby_connection(base_url: str, api_key: str) -> tuple[bool, str]:
    """GET /System/Info to verify connectivity and credentials.

    Returns (ok, message).
    """
    base = base_url.rstrip("/")
    try:
        resp = request_get(
            f"{base}/System/Info",
            headers=_auth_headers(api_key),
            timeout=(5, 10),
        )
        if resp.status_code == 200:
            try:
                data = resp.json()
                server_name = data.get("ServerName") or data.get("ProductName") or "服务器"
                version = data.get("Version", "")
                return True, f"连接成功：{server_name} {version}".strip()
            except Exception:
                return True, "连接成功"
        if resp.status_code == 401:
            return False, "API Key 无效或权限不足（HTTP 401）"
        if resp.status_code == 403:
            return False, "权限不足（HTTP 403）"
        return False, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)[:200]


class EmbyNotifier:
    """Debounced Emby / Jellyfin library refresh notifier.

    Collects successful scrape events and fires a single
    POST /Library/Refresh after a configurable quiet period,
    avoiding flooding the media server with repeated scan requests.
    """

    def __init__(self, cfg_getter: Callable[[], dict], delay: float = 30.0):
        """
        Parameters
        ----------
        cfg_getter : callable() -> dict
            Returns the current configuration dict (read fresh each time).
        delay : float
            Default quiet-period seconds after the last scrape before firing.
        """
        self._cfg_getter = cfg_getter
        self._default_delay = delay
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._pending_count = 0

    def notify_success(self, path: str = "") -> None:
        """Schedule a library refresh after a successful scrape.

        Calling this multiple times resets the quiet-period timer so that
        a burst of files results in exactly one refresh request.
        """
        cfg = self._cfg_getter()
        if not cfg.get("emby_notify_enabled"):
            return

        delay = float(
            cfg.get("emby_notify_delay", self._default_delay) or self._default_delay
        )

        with self._lock:
            self._pending_count += 1
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(delay, self._fire)
            self._timer.daemon = True
            self._timer.start()

    def _fire(self) -> None:
        with self._lock:
            count = self._pending_count
            self._pending_count = 0
            self._timer = None

        if count == 0:
            return

        cfg = self._cfg_getter()
        if not cfg.get("emby_notify_enabled"):
            return

        base_url = (cfg.get("emby_url") or "").strip()
        api_key = (cfg.get("emby_api_key") or "").strip()

        if not base_url or not api_key:
            logger.warning("Emby 通知已启用但 URL 或 API Key 未配置，跳过刷新")
            return

        ok, msg = trigger_library_refresh(base_url, api_key)
        if ok:
            logger.info("Emby 库扫描已触发（本批次 %d 个文件）：%s", count, msg)
        else:
            logger.warning("Emby 库扫描触发失败：%s", msg)
