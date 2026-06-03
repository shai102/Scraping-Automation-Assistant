import logging

from utils.app_runtime import TIMEOUT_IMAGE_DOWNLOAD
from utils.proxy import request_post


logger = logging.getLogger(__name__)

_TG_API = "https://api.telegram.org"


def send_photo(token: str, chat_id: str, photo_url: str, caption: str) -> dict:
    url = f"{_TG_API}/bot{token}/sendPhoto"
    resp = request_post(
        url,
        data={
            "chat_id": chat_id,
            "photo": photo_url,
            "caption": caption,
            "parse_mode": "HTML",
        },
        timeout=TIMEOUT_IMAGE_DOWNLOAD,
    )
    return resp.json()


def send_message(token: str, chat_id: str, text: str) -> dict:
    url = f"{_TG_API}/bot{token}/sendMessage"
    resp = request_post(
        url,
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        },
        timeout=TIMEOUT_IMAGE_DOWNLOAD,
    )
    return resp.json()


def send_test_message(token: str, chat_id: str) -> dict:
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
    sample_poster = "https://image.tmdb.org/t/p/w500/p4N0I6mIbqJVp5oBFGKfGCYxVOZ.jpg"
    try:
        result = send_photo(token, chat_id, sample_poster, caption)
        if result.get("ok"):
            return result
        return send_message(token, chat_id, caption)
    except Exception:
        return send_message(token, chat_id, caption)
