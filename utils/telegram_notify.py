"""Telegram notification compatibility facade."""

from utils.telegram_notify_batch import NotificationBatcher, send_batch
from utils.telegram_notify_caption import _MEDIA_EXTS, build_caption, get_poster_url
from utils.telegram_notify_sender import send_message, send_photo, send_test_message

_build_caption = build_caption
_get_poster_url = get_poster_url
_send_batch = send_batch
_send_message = send_message
_send_photo = send_photo
