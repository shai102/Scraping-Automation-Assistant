import re


EPISODE_NOISE_NUMBERS = {2160, 1080, 720, 480, 265, 264}
_DECIMAL_EP_RE = re.compile(
    r"""(?ix)
    (?:
        s\d{1,2}\s*e\d{1,4}\.\d(?!\d)
        | (?:ep?)\s*0*\d{1,4}\.\d(?!\d)
        | \u7b2c\s*0*\d{1,4}\.\d(?!\d)\s*[\u96c6\u8bdd\u8a71]
        | [\[\(\uff08]\s*0*\d{1,4}\.\d(?!\d)\s*[\]\)\uff09]
        | (?<![.\d])-\s*0*\d{1,2}\.\d(?!\d)(?=[\s\[\(\uff08]|$)
    )"""
)


def is_decimal_episode(pure_name):
    try:
        from guessit import guessit as _guessit

        guessed = _guessit(str(pure_name or ""))
        episode = guessed.get("episode")
        if isinstance(episode, list) and episode:
            episode = episode[0]
        if isinstance(episode, float) and episode != int(episode):
            return True
    except Exception:
        pass
    return bool(_DECIMAL_EP_RE.search(str(pure_name or "")))


def _coerce_episode_number(value):
    if isinstance(value, list) and value:
        value = value[0]
    if isinstance(value, (int, float)):
        num = int(value)
    elif isinstance(value, str) and value.strip().isdigit():
        num = int(value.strip())
    else:
        return None
    return num if 0 < num <= 5000 else None


def _is_episode_noise_number(num):
    return num in EPISODE_NOISE_NUMBERS or 1900 <= num <= 2099


def extract_episode_number(pure_name, guess_data=None, ai_data=None):
    text = str(pure_name or "")
    patterns = [
        r"(?i)\bS\d{1,2}E\s*0*(\d{1,4})\b",
        r"(?i)\bEP?\s*0*(\d{1,4})\b",
        r"(?i)第\s*0*(\d{1,4})\s*[集话話]\b",
        r"(?i)[\[\(（]\s*0*(\d{1,4})(?:v\d+)?\s*[\]\)）]",
        r"(?i)-\s*0*(\d{1,4})(?:v\d+)?(?=\s*(?:$|[\[\(（]))",
    ]
    for index, pattern in enumerate(patterns):
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            num = int(match.group(1))
        except Exception:
            continue
        if index >= 3 and _is_episode_noise_number(num):
            continue
        if 0 < num <= 5000:
            return num

    if guess_data:
        num = _coerce_episode_number(guess_data.get("episode"))
        if num and not _is_episode_noise_number(num):
            return num
    if ai_data:
        num = _coerce_episode_number(ai_data.get("episode"))
        if num and not _is_episode_noise_number(num):
            return num
    return None

