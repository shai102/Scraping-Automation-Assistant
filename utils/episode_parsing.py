import re
from functools import lru_cache


EPISODE_NOISE_NUMBERS = {2160, 1080, 720, 480, 265, 264}

# 小数集（总集篇 / 回顾集）检测 —— 全项目唯一定义处
DECIMAL_EPISODE_RE = re.compile(
    r"""(?ix)
    (?:
        s\d{1,2}\s*e\d{1,4}\.\d(?!\d)
        | (?:ep?)\s*0*\d{1,4}\.\d(?!\d)
        | 第\s*0*\d{1,4}\.\d(?!\d)\s*[集话話]
        | [\[\(（]\s*0*\d{1,4}\.\d(?!\d)\s*[\]\)）]
        | (?<![.\d])-\s*0*\d{1,2}\.\d(?!\d)(?=[\s\[\(（]|$)
    )"""
)

# 兼容旧名称（历史代码可能引用）
_DECIMAL_EP_RE = DECIMAL_EPISODE_RE

_CN_DIGITS = {
    "零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
    "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
}
_CN_UNITS = {"十": 10, "百": 100}
_CN_EPISODE_RE = re.compile(r"第\s*([零〇一二三四五六七八九十百两]{1,8})\s*[集话話]")

_EP_RANGE_PATTERNS = (
    # S01E01-E02 / S01E01E02 / S01E01-02
    re.compile(r"(?i)\bS\d{1,2}\s*E\s*0*(\d{1,4})\s*(?:-\s*E?|E)\s*0*(\d{1,4})\b"),
    # E01-E02 / EP01-02（无季前缀）
    re.compile(r"(?i)\bEP?\s*0*(\d{1,4})\s*-\s*(?:EP?\s*)?0*(\d{1,4})\b"),
    # 第01-02集 / 第01~02话 / 第01至02集
    re.compile(r"第\s*0*(\d{1,4})\s*[-~至]\s*0*(\d{1,4})\s*[集话話]"),
    # [01-02] / (01-02)
    re.compile(r"[\[\(（]\s*0*(\d{1,4})\s*[-~]\s*0*(\d{1,4})\s*[\]\)）]"),
)
_EP_RANGE_MAX_SPAN = 30


@lru_cache(maxsize=256)
def _cached_guessit_impl(name):
    try:
        from guessit import guessit as _guessit

        return dict(_guessit(name))
    except Exception:
        return {}


def cached_guessit(name):
    """带 LRU 缓存的 guessit，避免同一文件名在管线中被重复解析。"""
    return dict(_cached_guessit_impl(str(name or "")))


def cn_numeral_to_int(text):
    """中文数字转整数（支持 十/百 结构与逐位写法，如 十二、二十五、一百零三、二〇五）。"""
    raw = str(text or "").strip()
    if not raw or len(raw) > 8:
        return None
    if all(ch in _CN_DIGITS for ch in raw):
        value = 0
        for ch in raw:
            value = value * 10 + _CN_DIGITS[ch]
        return value if 0 < value <= 2000 else None
    total = 0
    num = 0
    for ch in raw:
        if ch in _CN_DIGITS:
            num = _CN_DIGITS[ch]
        elif ch in _CN_UNITS:
            unit = _CN_UNITS[ch]
            if num == 0:
                num = 1
            total += num * unit
            num = 0
        else:
            return None
    total += num
    return total if 0 < total <= 2000 else None


def is_decimal_episode(pure_name, guess_data=None):
    guessed = guess_data if isinstance(guess_data, dict) else cached_guessit(pure_name)
    episode = guessed.get("episode")
    if isinstance(episode, list) and episode:
        episode = episode[0]
    if isinstance(episode, float) and episode != int(episode):
        return True
    return bool(DECIMAL_EPISODE_RE.search(str(pure_name or "")))


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


def extract_episode_range(pure_name, guess_data=None):
    """提取多集合并文件的集数范围，返回 (start, end) 或 None。

    支持 S01E01-E02 / S01E01E02 / EP01-02 / 第01-02话 / [01-02]，
    以及 guessit 解析出的连续集数列表。
    """
    text = str(pure_name or "")
    for index, pattern in enumerate(_EP_RANGE_PATTERNS):
        match = pattern.search(text)
        if not match:
            continue
        try:
            start, end = int(match.group(1)), int(match.group(2))
        except Exception:
            continue
        if index >= 3 and (_is_episode_noise_number(start) or _is_episode_noise_number(end)):
            continue
        if 1 <= start < end and end - start <= _EP_RANGE_MAX_SPAN:
            return start, end

    episodes = (guess_data or {}).get("episode") if isinstance(guess_data, dict) else None
    if isinstance(episodes, list) and len(episodes) >= 2:
        try:
            nums = sorted(int(ep) for ep in episodes)
        except Exception:
            return None
        if (
            nums[0] >= 1
            and nums == list(range(nums[0], nums[-1] + 1))
            and nums[-1] - nums[0] <= _EP_RANGE_MAX_SPAN
            and not any(_is_episode_noise_number(n) for n in nums)
        ):
            return nums[0], nums[-1]
    return None


def extract_episode_number(pure_name, guess_data=None, ai_data=None):
    text = str(pure_name or "")
    patterns = [
        r"(?i)\bS\d{1,2}E\s*0*(\d{1,4})\b",
        r"(?i)\bEP?\s*0*(\d{1,4})\b",
        r"(?i)第\s*0*(\d{1,4})(?:\s*[-~至]\s*0*\d{1,4})?\s*[集话話]\b",
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

    cn_match = _CN_EPISODE_RE.search(text)
    if cn_match:
        num = cn_numeral_to_int(cn_match.group(1))
        if num:
            return num

    ep_range = extract_episode_range(text, guess_data)
    if ep_range:
        return ep_range[0]

    if guess_data:
        num = _coerce_episode_number(guess_data.get("episode"))
        if num and not _is_episode_noise_number(num):
            return num
    if ai_data:
        num = _coerce_episode_number(ai_data.get("episode"))
        if num and not _is_episode_noise_number(num):
            return num
    return None
