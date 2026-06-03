import logging
import threading
import time
from collections import deque

from utils.proxy import session


AI_RATE_LIMIT_MAX_REQUESTS = 20
AI_RATE_LIMIT_WINDOW_SECONDS = 60.0
DISABLE_REASONING_PARAMS = {
    "thinking": {"type": "disabled"},
}
_ai_request_lock = threading.Lock()
_ai_request_times = deque()


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


def throttle_ai_request():
    """Throttle outbound AI requests to stay under minute caps."""
    while True:
        wait_seconds = 0.0
        with _ai_request_lock:
            now = time.monotonic()
            while (
                _ai_request_times
                and now - _ai_request_times[0] >= AI_RATE_LIMIT_WINDOW_SECONDS
            ):
                _ai_request_times.popleft()

            if len(_ai_request_times) < AI_RATE_LIMIT_MAX_REQUESTS:
                _ai_request_times.append(now)
                return

            wait_seconds = AI_RATE_LIMIT_WINDOW_SECONDS - (now - _ai_request_times[0])

        time.sleep(max(wait_seconds, 0.05))


def extract_text_from_content(value):
    """Extract plain text from OpenAI-compatible content variants."""
    if isinstance(value, str):
        return value.strip()

    if isinstance(value, list):
        parts = []
        for item in value:
            text = extract_text_from_content(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()

    if isinstance(value, dict):
        for key in ("text", "content", "value", "reasoning", "reasoning_content"):
            text = extract_text_from_content(value.get(key))
            if text:
                return text
        return ""

    return ""


def with_disabled_reasoning(payload):
    data = dict(payload or {})
    data.update(DISABLE_REASONING_PARAMS)
    return data


def without_disabled_reasoning(payload):
    data = dict(payload or {})
    for key in DISABLE_REASONING_PARAMS:
        data.pop(key, None)
    return data


def should_retry_without_disabled_reasoning(response):
    status = getattr(response, "status_code", None)
    if status != 400:
        return False

    text = response_body_snippet(response, 1000).lower()
    if "reasoning" not in text and "thinking" not in text and "think" not in text:
        return False

    markers = (
        "mandatory",
        "cannot be disabled",
        "can't be disabled",
        "unsupported",
        "unknown parameter",
        "unrecognized",
        "invalid parameter",
    )
    return any(marker in text for marker in markers)


def post_openai_compatible(url, payload, headers, timeout):
    throttle_ai_request()
    response = session.post(url, json=payload, headers=headers, timeout=timeout)
    if should_retry_without_disabled_reasoning(response):
        logging.info("AI provider requires reasoning; retrying without disable flags")
        throttle_ai_request()
        response = session.post(
            url,
            json=without_disabled_reasoning(payload),
            headers=headers,
            timeout=timeout,
        )
    return response


def extract_openai_message_content(payload):
    if not isinstance(payload, dict):
        raise ValueError("AI响应不是JSON对象")

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("AI响应缺少choices")

    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise ValueError("AI响应choices结构无效")

    message = first_choice.get("message")
    if not isinstance(message, dict):
        raise ValueError("AI响应缺少message")

    content = extract_text_from_content(message.get("content"))
    if not content:
        for key in ("reasoning_content", "reasoning", "output_text"):
            content = extract_text_from_content(message.get(key))
            if content:
                break
    if not content:
        content = extract_text_from_content(first_choice.get("text"))
    if not content:
        content = extract_text_from_content(payload.get("output_text"))
    if not content:
        raise ValueError("AI响应content为空")

    return content.strip()


def normalize_top_p(value, default=0.9):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(1.0, number))


def normalize_temperature(value, default=0.2):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return float(default)
    return max(0.0, min(2.0, number))

