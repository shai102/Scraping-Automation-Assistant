import difflib
import logging
import os
import re

from utils.helpers import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    VERSION_TAG_RE,
    clean_search_title,
    derive_title_from_filename,
    normalize_compare_text,
    parse_error_message,
    safe_int,
    safe_str,
)

MEDIA_SUFFIX_START_RE = re.compile(
    r"""(?ix)
    (?:^|[.\s_\-\[\(])
    (
        \d{3,4}p
        |web[.\s_-]?dl
        |web[.\s_-]?rip
        |blu[.\s_-]?ray
        |bluray
        |bdrip
        |bdremux
        |remux
        |hdtv
        |hdrip
        |dvdrip
        |uhd
        |hevc
        |x265
        |x264
        |h[.\s_-]?265
        |h[.\s_-]?264
        |av1
        |hdr10\+?
        |dolby[.\s_-]?vision
        |dv
        |aac(?:[.\-_]?\d\.\d)?
        |ddp(?:[.\-_]?\d\.\d)?
        |dd(?:[.\-_]?\d\.\d)?
        |dts(?:[.\-_]?hd)?
        |truehd
        |atmos
        |tving
        |nf
        |netflix
        |amzn
        |amazon
        |dsnp
        |disney
        |hmax
        |hulu
        |colortv
    )
    """,
)


def extract_lang_and_ext(filename, lang_tags):
    """Extract language suffix and extension from a media name."""
    tags = str(lang_tags or "").strip()
    if not tags:
        return os.path.splitext(filename)

    tag_items = [t.strip() for t in tags.split("|") if t.strip()]
    if not tag_items:
        return os.path.splitext(filename)

    safe_tags = "|".join(re.escape(t) for t in tag_items)
    pattern = rf"(\.(?:{safe_tags}))?(\.[a-z0-9]+)$"
    try:
        regex = re.compile(pattern, re.I)
    except re.error:
        return os.path.splitext(filename)

    match = regex.search(filename)
    if match and match.group(1):
        return filename[: match.start()], match.group(1) + match.group(2)
    return os.path.splitext(filename)


def extract_media_suffix(filename, pure_name=None):
    """Extract a media-info suffix like 2160p.WEB-DL.H265.AAC-Group."""
    text = str(
        pure_name
        if pure_name not in (None, "")
        else os.path.splitext(str(filename or ""))[0]
    ).strip()
    if not text:
        return ""

    match = MEDIA_SUFFIX_START_RE.search(text)
    if not match:
        return ""

    suffix = text[match.start(1):].strip(" ._-[]()")
    if not suffix:
        return ""
    if normalize_compare_text(suffix) == normalize_compare_text(text):
        return ""
    return suffix


def cleanup_rendered_filename(text):
    """Normalize rendered filename text and remove empty separator fragments."""
    cleaned = str(text or "")
    cleaned = re.sub(r"\s*[\(\[]\s*[\)\]]", "", cleaned)
    cleaned = re.sub(r"\s*\{\s*\}", "", cleaned)
    cleaned = re.sub(r"\s*\(\s*\)", "", cleaned)
    cleaned = re.sub(r"\s*-\s*(?=\.)|\s*-\s*$", "", cleaned)
    cleaned = re.sub(r"\s+(?=\.)", "", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    return cleaned.strip()


def apply_media_suffix_template(template, media_suffix, preserve_media_suffix):
    """Auto-append media suffix before extension when enabled and template omits it."""
    working = str(template or "")
    suffix = str(media_suffix or "").strip()
    if preserve_media_suffix and suffix and "{media_suffix}" not in working:
        if "{ext}" in working:
            working = working.replace("{ext}", " - {media_suffix}{ext}", 1)
        else:
            working = working + " - {media_suffix}"
    return working


def _is_jinja2_template(template):
    """Return True if the template string uses Jinja2 syntax ({{ or {%)."""
    return bool(re.search(r'\{\{|\{%', str(template or "")))


def _render_jinja2(template, context):
    """Render a Jinja2 template string with the given context.

    Uses SandboxedEnvironment for safety.  On any render error, falls back to
    returning the raw template string so the user can notice and fix it.
    """
    try:
        from jinja2.sandbox import SandboxedEnvironment
        from jinja2 import Undefined
        env = SandboxedEnvironment(undefined=Undefined, autoescape=False)
        rendered = env.from_string(str(template)).render(**context)
        return cleanup_rendered_filename(rendered)
    except Exception as err:
        logging.warning("Jinja2 模板渲染失败，回退保留原模板: %s", err)
        return cleanup_rendered_filename(str(template))


def render_filename_template(template, context, preserve_media_suffix=False):
    """Render filename templates supporting legacy {var} and Jinja2 {{ }}/ {% %} syntax.

    Detection:
    - Template contains ``{{`` or ``{%``  → Jinja2 path (SandboxedEnvironment)
    - Otherwise                           → Legacy .replace() path (backward-compat)
    """
    context = context or {}
    media_suffix = safe_str(context.get("media_suffix"))
    working = apply_media_suffix_template(
        template, media_suffix, preserve_media_suffix
    )

    if _is_jinja2_template(working):
        return _render_jinja2(working, context)

    # --- Legacy path (unchanged) ---
    rendered = (
        str(working)
        .replace("{title}", safe_str(context.get("title")))
        .replace("{year}", safe_str(context.get("year")))
        .replace("{s:02d}", safe_str(context.get("season")))
        .replace("{s}", safe_str(context.get("season")))
        .replace("{season}", safe_str(context.get("season")))
        .replace("{e:02d}", safe_str(context.get("episode")))
        .replace("{e}", safe_str(context.get("episode")))
        .replace("{episode}", safe_str(context.get("episode")))
        .replace("{ep_name}", safe_str(context.get("ep_name")))
        .replace("{media_suffix}", media_suffix)
        .replace("{ext}", safe_str(context.get("ext")))
    )
    return cleanup_rendered_filename(rendered)


def extract_explicit_season(pure_name):
    """Only parse explicit season markers to avoid treating years as seasons.

    S-prefixed patterns (S00E01, S00) unambiguously denote season 0 (specials)
    and are allowed to return 0.  Other patterns (Season N, 第N季, Nth Season)
    must be >= 1 to avoid misidentifying year-like numbers.
    """
    text = str(pure_name or "")
    # S-prefixed patterns are always unambiguous — allow season 0
    s_prefix_patterns = [
        r"(?i)\bS\s*0*(\d{1,2})\s*E\s*0*\d{1,4}\b",
        r"(?i)\bS\s*0*(\d{1,2})\b",
    ]
    for pattern in s_prefix_patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        season_num = safe_int(match.group(1), -1)
        if 0 <= season_num <= 99:
            return season_num
    # Other patterns must be >= 1 to avoid false-positives
    other_patterns = [
        r"(?i)\bSeason\s*0*(\d{1,2})\b",
        r"(?i)\b(\d{1,2})(?:st|nd|rd|th)\s*Season\b",
        r"第\s*0*(\d{1,2})\s*季",
    ]
    for pattern in other_patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        season_num = safe_int(match.group(1), 0)
        if 1 <= season_num <= 99:
            return season_num
    return None


def extract_season_from_dir(dir_path):
    """Extract season number from the innermost folder component of dir_path.

    Matches patterns used by Emby/Jellyfin/Kodi library layout:
      Season 1, Season 01, Season 0 (specials), 第1季, 第01季
    Returns an int (including 0 for specials) on success, or None if no match.
    """
    folder_name = os.path.basename(str(dir_path or ""))
    patterns = [
        r"(?i)\bSeason\s*0*(\d{1,2})\b",
        r"第\s*0*(\d{1,2})\s*季",
    ]
    for pattern in patterns:
        m = re.search(pattern, folder_name)
        if m:
            n = safe_int(m.group(1), -1)
            if 0 <= n <= 99:
                return n
    return None


def pick_season(pure_name, guess_data=None, fallback=1):
    """Prefer explicit season marker, then sane guessed season, then fallback."""
    explicit = extract_explicit_season(pure_name)
    if explicit is not None:
        return explicit

    guessed = safe_int((guess_data or {}).get("season"), 0)
    if 0 < guessed <= 99:
        return guessed

    fallback_num = safe_int(fallback, 1)
    if 1 <= fallback_num <= 99:
        return fallback_num
    return 1


def can_reuse_dir_ai(cached_ai, pure_name, guess_data=None):
    """Allow directory-level AI cache reuse only for clearly same title/year."""
    if not isinstance(cached_ai, dict):
        return False

    cached_titles = [clean_search_title(cached_ai.get("title") or "")]
    for alias in cached_ai.get("title_aliases") or []:
        cached_titles.append(clean_search_title(alias or ""))

    cached_keys = [normalize_compare_text(title) for title in cached_titles]
    cached_keys = [key for key in cached_keys if key]
    if not cached_keys:
        return False

    cached_year = safe_str(cached_ai.get("year"))
    guess_year = safe_str((guess_data or {}).get("year"))
    if cached_year and guess_year and cached_year != guess_year:
        return False

    title_candidates = [
        clean_search_title((guess_data or {}).get("title") or ""),
        derive_title_from_filename(pure_name),
    ]

    for candidate in title_candidates:
        cand_key = normalize_compare_text(candidate)
        if not cand_key:
            continue
        for cached_key in cached_keys:
            if cand_key == cached_key:
                return True
            if len(cand_key) >= 4 and len(cached_key) >= 4:
                ratio = difflib.SequenceMatcher(None, cand_key, cached_key).ratio()
                if ratio >= 0.85:
                    return True
                # 处理 guessit 剥离 OVA/SP 等标签后标题变短的情况：
                # 若其中一方是另一方的前缀，也视为同一作品（如"骑士团"与"骑士团 OVA"）
                shorter, longer = (
                    (cand_key, cached_key)
                    if len(cand_key) <= len(cached_key)
                    else (cached_key, cand_key)
                )
                if longer.startswith(shorter) and len(shorter) >= 4:
                    return True

    return False


def get_version_tag(path):
    match = VERSION_TAG_RE.search(os.path.basename(path))
    return f" {match.group(0)}" if match else ""


def friendly_status_text(message):
    """Render coded errors to concise Chinese status text for UI display."""
    raw_text = str(message or "").strip()
    if not raw_text:
        return ""

    has_error_hint = (
        ":" in raw_text
        or any(
            token in raw_text
            for token in (
                "超时",
                "未配置",
                "HTTP",
                "解析失败",
                "JSON",
                "无结果",
                "未匹配",
                "无效",
                "失败",
                "异常",
                "错误",
            )
        )
    )
    if not has_error_hint:
        return raw_text

    code, detail = parse_error_message(message)
    if not code:
        return raw_text

    if code == ERROR_CODE_HTTP and (
        "429" in raw_text.lower() or "rate limit" in raw_text.lower()
    ):
        return "AI接口限流，请稍后重试"

    template = {
        ERROR_CODE_TIMEOUT: "请求超时，请稍后重试",
        ERROR_CODE_CONFIG: "配置缺失，请检查密钥设置",
        ERROR_CODE_HTTP: "接口请求失败，请检查网络或服务状态",
        ERROR_CODE_PARSE: "返回解析失败，请稍后重试",
        ERROR_CODE_NO_RESULT: "未找到匹配结果",
        ERROR_CODE_INVALID: "输入无效或资源不存在",
        ERROR_CODE_UNKNOWN: "处理失败，请查看日志",
    }.get(code, "处理失败，请查看日志")

    if detail and code in {ERROR_CODE_PARSE, ERROR_CODE_HTTP, ERROR_CODE_UNKNOWN}:
        compact_detail = " ".join(str(detail).split())
        return f"{template} (返回: {compact_detail[:60]})"
    return template


def build_status_text(*messages):
    raw_parts = [str(m).strip() for m in messages if str(m or "").strip()]
    if not raw_parts:
        return ""

    friendly_parts = [friendly_status_text(m) for m in raw_parts]
    merged = list(dict.fromkeys(friendly_parts))
    return " / ".join(merged)
