"""Telegram notification — batch-aggregates successful scrape results
and sends a single notification per media (same TMDB ID + season) after
a configurable quiet period.
"""

import logging
import os
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils.helpers import TIMEOUT_IMAGE_DOWNLOAD, request_post

logger = logging.getLogger(__name__)

_TG_API = "https://api.telegram.org"

# 统计文件数时认定为媒体的扩展名
_MEDIA_EXTS = {
    '.strm', '.mp4', '.mkv', '.ts', '.iso', '.rmvb', '.avi', '.mov',
    '.mpeg', '.mpg', '.wmv', '.3gp', '.asf', '.m4v', '.flv', '.m2ts',
    '.tp', '.f4v',
}


# ------------------------------------------------------------------
# Low-level send helpers
# ------------------------------------------------------------------

def _send_photo(token: str, chat_id: str, photo_url: str, caption: str) -> dict:
    """Send a photo message via Telegram Bot API."""
    url = f"{_TG_API}/bot{token}/sendPhoto"
    resp = request_post(url, data={
        "chat_id": chat_id,
        "photo": photo_url,
        "caption": caption,
        "parse_mode": "HTML",
    }, timeout=TIMEOUT_IMAGE_DOWNLOAD)
    return resp.json()


def _send_message(token: str, chat_id: str, text: str) -> dict:
    """Send a plain text message via Telegram Bot API."""
    url = f"{_TG_API}/bot{token}/sendMessage"
    resp = request_post(url, data={
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }, timeout=TIMEOUT_IMAGE_DOWNLOAD)
    return resp.json()


def send_test_message(token: str, chat_id: str) -> dict:
    """Send a test notification using the same format as a real scrape notification."""
    caption = (
        "🖥 新片入库：  刮削助手通知测试-(2024) S01 E01-E03\n"
        "\n"
        "📁 分类：动漫、科幻\n"
        "📂 来源：Downloads\n"
        "📄 本次入库：3集\n"
        "📅 本季：S01 已有10集，缺2集（共12集）\n"
        "🎬 影号：100565\n"
        "\n"
        "✨「又有新片可以看了，快来探索吧。」"
    )
    # Use a known TMDB poster as sample image
    sample_poster = "https://image.tmdb.org/t/p/w500/p4N0I6mIbqJVp5oBFGKfGCYxVOZ.jpg"
    try:
        result = _send_photo(token, chat_id, sample_poster, caption)
        if result.get("ok"):
            return result
        # Fallback to text if photo fails
        return _send_message(token, chat_id, caption)
    except Exception:
        return _send_message(token, chat_id, caption)


# ------------------------------------------------------------------
# Build caption from a batch of items
# ------------------------------------------------------------------

def _build_caption(folder_name: str, items: list, total_ep: int, file_count: int = 0, existing_count: int = 0) -> str:
    """Build the notification caption text matching the screenshot format.

    Parameters
    ----------
    folder_name : str
        Basename of the monitored folder (used as 来源).
    items : list[MediaItem]
        All successfully scraped items sharing the same (tmdb_id, season).
    total_ep : int
        Total episodes in this season (from TMDB API), 0 if unknown.
    file_count : int
        Number of items in this batch (本次入库集数). 0 = fallback to len(items).
    existing_count : int
        Current media file count in the season folder (已有集数，用于缺集对比).
    """
    meta = items[0].metadata or {}
    title = meta.get("title", "未知")
    year = meta.get("year", "")
    media_type = meta.get("type", "")
    season = meta.get("s")
    tmdb_id = meta.get("id", "")
    genres = meta.get("genres", "")
    if isinstance(genres, list):
        genres = "、".join(genres)

    if not file_count:
        file_count = len(items)

    # Title line: 标题 (年份) S01 E01-E23
    title_line = f"{title}"
    if year:
        title_line += f"-({year})"

    if media_type == "episode" and season is not None:
        s_str = f"S{int(season):02d}"
        episodes = sorted(set(int(it.metadata.get("e", 0)) for it in items if it.metadata and it.metadata.get("e")))
        if episodes:
            if len(episodes) == 1:
                ep_range = f"E{episodes[0]:02d}"
            else:
                ep_range = f"E{episodes[0]:02d}-E{episodes[-1]:02d}"
            title_line += f" {s_str} {ep_range}"
        else:
            title_line += f" {s_str}"

    lines = [f"🖥 新片入库：  {title_line}"]

    # 分类
    if genres:
        lines.append(f"📁 分类：{genres}")

    # 来源
    if folder_name:
        lines.append(f"📂 来源：{folder_name}")

    # 本次入库集数
    lines.append(f"📄 本次入库：{file_count}集")

    # 本季信息 (TV only)
    if media_type == "episode" and season is not None:
        s_str = f"S{int(season):02d}"
        if total_ep > 0:
            missing = total_ep - existing_count
            if missing > 0:
                lines.append(f"📅 本季：{s_str} 已有{existing_count}集，缺{missing}集（共{total_ep}集）")
            else:
                lines.append(f"📅 本季：{s_str} 已有{existing_count}集，共{total_ep}集")
        elif existing_count > 0:
            lines.append(f"📅 本季：{s_str} 已有{existing_count}集")
        else:
            lines.append(f"📅 本季：{s_str}")

    # 影号
    if tmdb_id and str(tmdb_id) != "None":
        lines.append(f"🎬 影号：{tmdb_id}")

    # Footer
    lines.append("")
    lines.append('✨「又有新片可以看了，快来探索吧。」')

    return "\n".join(lines)


def _get_poster_url(items: list) -> str:
    """Extract a usable poster image URL from item metadata."""
    for item in items:
        meta = item.metadata or {}
        # Try season poster first, then show poster
        for key in ("s_poster", "poster"):
            val = meta.get(key, "")
            if val:
                if val.startswith("http"):
                    return val
                # TMDB poster_path like /xxxx.jpg
                return f"https://image.tmdb.org/t/p/w500{val}"
    return ""


# ------------------------------------------------------------------
# Batch sender (called by the timer)
# ------------------------------------------------------------------

def _send_batch(folder_name: str, items: list, cfg: dict, season_folder: str = ""):
    """Send one aggregated TG notification for a batch of items."""
    token = (cfg.get("tg_bot_token") or "").strip()
    chat_id = (cfg.get("tg_chat_id") or "").strip()
    if not token or not chat_id:
        return

    # Fetch total episode count for TV
    total_ep = 0
    meta = items[0].metadata or {}
    if meta.get("type") == "episode" and meta.get("provider") == "tmdb":
        try:
            from db.tmdb_api import fetch_tmdb_season_episode_count
            tmdb_key = (cfg.get("tmdb_api_key") or "").strip()
            if tmdb_key and meta.get("id") and meta.get("s") is not None:
                total_ep = fetch_tmdb_season_episode_count(
                    str(meta["id"]), int(meta["s"]), tmdb_key
                )
        except Exception as e:
            logger.debug(f"获取季集数失败: {e}")

    # 统计目标文件夹内已有的媒体文件数量（用于「已有/共N集」缺集对比）
    existing_count = 0
    if season_folder and os.path.isdir(season_folder):
        try:
            existing_count = sum(1 for f in os.listdir(season_folder)
                                 if os.path.splitext(f)[1].lower() in _MEDIA_EXTS)
        except Exception:
            pass

    caption = _build_caption(folder_name, items, total_ep, file_count=len(items), existing_count=existing_count)
    poster_url = _get_poster_url(items)

    try:
        if poster_url:
            result = _send_photo(token, chat_id, poster_url, caption)
        else:
            result = _send_message(token, chat_id, caption)

        if not result.get("ok"):
            logger.warning(f"TG 通知发送失败: {result.get('description', result)}")
        else:
            logger.info(f"TG 通知已发送: {meta.get('title', '?')}")
    except Exception as e:
        logger.warning(f"TG 通知异常: {e}")


# ------------------------------------------------------------------
# NotificationBatcher — groups items and fires after quiet period
# ------------------------------------------------------------------

class NotificationBatcher:
    """Thread-safe batcher that groups successful scrape items by
    (folder_id, tmdb_id, season) and fires a single TG notification
    after *delay* seconds of inactivity for each group.
    """

    def __init__(self, cfg_getter: Callable[[], dict], delay: float = 300.0):
        """
        Parameters
        ----------
        cfg_getter : callable() -> dict
            Returns the current configuration dict (read fresh each time).
        delay : float
            Seconds to wait after the last item before sending.
        """
        self._cfg_getter = cfg_getter
        self._default_delay = delay
        self._lock = threading.Lock()
        # key -> {"folder_name": str, "items": dict[ep_key -> item], "timer": Timer, "season_folder": str}
        # items is a dict to deduplicate: same episode arriving multiple times
        # (e.g. from sidecar re-detection) overwrites instead of appending.
        self._groups: Dict[Tuple, dict] = {}

    def add(self, folder_id: int, folder_name: str, item: Any):
        """Add a successfully scraped item. Resets the quiet timer for its group."""
        cfg = self._cfg_getter()
        if not cfg.get("tg_notify_enabled"):
            return

        meta = item.metadata or {}
        tmdb_id = str(meta.get("id", "None"))
        season = str(meta.get("s", "0"))
        key = (folder_id, tmdb_id, season)

        delay = float(cfg.get("tg_notify_delay", self._default_delay) or self._default_delay)

        # Compute a dedup key: episode number for TV, item.id for movies
        media_type = meta.get("type", "")
        ep = meta.get("e")
        if media_type == "episode" and ep is not None:
            ep_key = int(ep)
        else:
            ep_key = getattr(item, "id", id(item))

        # season_folder: 目标 Season 目录，用于统计实际 STRM 数量
        season_folder = ""
        item_path = getattr(item, 'path', None)
        if item_path:
            season_folder = os.path.dirname(item_path)

        with self._lock:
            if key not in self._groups:
                self._groups[key] = {
                    "folder_name": folder_name,
                    "items": {},
                    "timer": None,
                    "season_folder": season_folder,
                }
            group = self._groups[key]
            # Overwrite if same episode arrives again (dedup)
            group["items"][ep_key] = item
            if season_folder and os.path.isdir(season_folder):
                group["season_folder"] = season_folder

            # 始终等待安静期结束后再发送，避免每集单独触发通知
            if group["timer"] is not None:
                group["timer"].cancel()
            group["timer"] = threading.Timer(delay, self._fire, args=(key,))
            group["timer"].daemon = True
            group["timer"].start()

    def _fire(self, key: Tuple):
        """Timer callback — pop the group and send."""
        with self._lock:
            group = self._groups.pop(key, None)
        if not group or not group["items"]:
            return
        # Convert dedup dict to sorted list (by episode number)
        group["items"] = sorted(
            group["items"].values(),
            key=lambda it: int((it.metadata or {}).get("e") or 0)
        )
        season_folder = group.get("season_folder", "")

        cfg = self._cfg_getter()
        if not cfg.get("tg_notify_enabled"):
            return

        try:
            _send_batch(group["folder_name"], group["items"], cfg, season_folder=season_folder)
        except Exception as e:
            logger.debug(f"TG 批量通知失败: {e}")
