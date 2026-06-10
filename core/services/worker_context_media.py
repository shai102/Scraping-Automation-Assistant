import re

from core.services.naming_templates import extract_media_suffix, render_filename_template
from utils.value_utils import safe_filename


# 明确的剧集标记（含中文数字集数），用于纠正 guessit 的电影误判
_EXPLICIT_EP_MARKER_RE = re.compile(
    r"(?i)(?:\bS\d{1,2}E\d{1,4}\b"
    r"|\bEP?\s*\d{1,4}\b"
    r"|第\s*(?:\d{1,4}|[零〇一二三四五六七八九十百两]{1,8})\s*[集话話])"
)


def render_media_filename(
    ctx,
    template,
    *,
    title="",
    year="",
    season="",
    episode="",
    ep_name="",
    ext="",
    source_filename="",
    pure_name="",
    parse_source="",
    source_provider="",
    media_id="",
    is_tv=True,
    original_title="",
    rating=0,
    genres=None,
    studios=None,
    overview="",
    ep_plot="",
    release="",
):
    preserve = bool(ctx.preserve_media_suffix.get())
    media_suffix = ""
    if preserve:
        media_suffix = safe_filename(extract_media_suffix(source_filename, pure_name))
    context = {
        "title": title,
        "year": year,
        "season": season,
        "episode": episode,
        "ep_name": ep_name,
        "ext": ext,
        "media_suffix": media_suffix,
        "parse_source": parse_source,
        "source_provider": source_provider,
        "media_id": media_id,
        "is_tv": is_tv,
        "original_title": original_title,
        "rating": rating or 0,
        "genres": genres or [],
        "studios": studios or [],
        "overview": overview,
        "ep_plot": ep_plot,
        "release": release,
    }
    return render_filename_template(template, context, preserve), media_suffix


def resolve_media_type(ctx, guess_data=None, pure_name=None, extracted_ep=None):
    override = str(ctx.media_type_override.get() or "").strip()
    if override == "电影":
        return "movie"
    if override == "电视剧":
        return "episode"
    guessed_type = str((guess_data or {}).get("type") or "").strip().lower()
    if guessed_type in ("movie", "film"):
        # guessit 不认识中文集数等标记时会误判为电影，这里用明确标记纠偏
        if pure_name and _EXPLICIT_EP_MARKER_RE.search(str(pure_name)):
            return "episode"
        return "movie"
    if guessed_type == "episode":
        return "episode"
    if pure_name is not None:
        text = str(pure_name or "")
        has_season_ep = bool(re.search(r"(?i)\bS\d{1,2}E\d{1,4}\b", text))
        has_ep_marker = bool(_EXPLICIT_EP_MARKER_RE.search(text))
        has_season_marker = bool(re.search(r"(?i)(?:\bS\d{1,2}\b|Season\s*\d|第\s*\d{1,2}\s*季)", text))
        if has_season_ep or has_ep_marker or has_season_marker:
            return "episode"
        if extracted_ep is None:
            return "movie"
    return "episode"
