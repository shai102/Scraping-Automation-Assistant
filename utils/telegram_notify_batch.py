import logging
import os
import threading
from typing import Any, Callable, Dict, Tuple

from utils.telegram_notify_caption import _MEDIA_EXTS, build_caption, get_poster_url
from utils.telegram_notify_sender import send_message, send_photo


logger = logging.getLogger(__name__)


def send_batch(folder_name: str, items: list, cfg: dict, season_folder: str = ""):
    token = (cfg.get("tg_bot_token") or "").strip()
    chat_id = (cfg.get("tg_chat_id") or "").strip()
    if not token or not chat_id:
        return

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
        except Exception as err:
            logger.debug(f"获取季集数失败: {err}")

    existing_count = 0
    if season_folder and os.path.isdir(season_folder):
        try:
            existing_count = sum(
                1
                for f in os.listdir(season_folder)
                if os.path.splitext(f)[1].lower() in _MEDIA_EXTS
            )
        except Exception:
            pass

    media_item_count = (
        sum(
            1
            for it in items
            if os.path.splitext(
                getattr(it, "old_name", "") or getattr(it, "path", "") or ""
            )[1].lower()
            in _MEDIA_EXTS
        )
        or len(items)
    )
    caption = build_caption(
        folder_name,
        items,
        total_ep,
        file_count=media_item_count,
        existing_count=existing_count,
    )
    poster_url = get_poster_url(items)

    try:
        if poster_url:
            result = send_photo(token, chat_id, poster_url, caption)
        else:
            result = send_message(token, chat_id, caption)

        if not result.get("ok"):
            logger.warning(f"TG 通知发送失败: {result.get('description', result)}")
        else:
            logger.info(f"TG 通知已发送: {meta.get('title', '?')}")
    except Exception as err:
        logger.warning(f"TG 通知异常: {err}")


class NotificationBatcher:
    def __init__(self, cfg_getter: Callable[[], dict], delay: float = 300.0):
        self._cfg_getter = cfg_getter
        self._default_delay = delay
        self._lock = threading.Lock()
        self._groups: Dict[Tuple, dict] = {}

    def add(self, folder_id: int, folder_name: str, item: Any):
        cfg = self._cfg_getter()
        if not cfg.get("tg_notify_enabled"):
            return

        meta = item.metadata or {}
        tmdb_id = str(meta.get("id", "None"))
        season = str(meta.get("s", "0"))
        key = (folder_id, tmdb_id, season)
        delay = float(cfg.get("tg_notify_delay", self._default_delay) or self._default_delay)

        media_type = meta.get("type", "")
        ep = meta.get("e")
        if media_type == "episode" and ep is not None:
            ep_key = int(ep)
        else:
            ep_key = getattr(item, "id", id(item))

        season_folder = ""
        item_path = getattr(item, "path", None)
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
            group["items"][ep_key] = item
            if season_folder and os.path.isdir(season_folder):
                group["season_folder"] = season_folder

            if group["timer"] is not None:
                group["timer"].cancel()
            group["timer"] = threading.Timer(delay, self._fire, args=(key,))
            group["timer"].daemon = True
            group["timer"].start()

    def _fire(self, key: Tuple):
        with self._lock:
            group = self._groups.pop(key, None)
        if not group or not group["items"]:
            return

        group["items"] = sorted(
            group["items"].values(),
            key=lambda it: int((it.metadata or {}).get("e") or 0),
        )
        season_folder = group.get("season_folder", "")
        cfg = self._cfg_getter()
        if not cfg.get("tg_notify_enabled"):
            return

        try:
            send_batch(group["folder_name"], group["items"], cfg, season_folder=season_folder)
        except Exception as err:
            logger.debug(f"TG 批量通知失败: {err}")
