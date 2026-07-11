import logging
import os

from core.models.recognition_result import RecognitionResult


logger = logging.getLogger(__name__)


def try_nfo_fast_path(item, ctx) -> bool:
    """Resolve subtitle/audio metadata from an existing tvshow.nfo when possible."""
    import xml.etree.ElementTree as ET

    from core.services.naming_templates import extract_lang_and_ext
    from db.tmdb_api import (
        fetch_hybrid_episode_meta,
        fetch_tmdb_episode_meta,
        fetch_tmdb_season_poster,
    )
    from utils.library_paths import build_existing_library_target
    from utils.title_parsing import cached_guessit, extract_episode_number, extract_episode_range
    from utils.value_utils import safe_filename

    file_dir = os.path.dirname(item.path)
    search_dirs = [file_dir, os.path.dirname(file_dir)]
    nfo_path = None
    for candidate_dir in search_dirs:
        candidate = os.path.join(candidate_dir, "tvshow.nfo")
        if os.path.isfile(candidate):
            nfo_path = candidate
            break
    if not nfo_path:
        return False

    try:
        tree = ET.parse(nfo_path)
        root_el = tree.getroot()
    except Exception:
        return False

    tmdb_id, bgm_id = "", ""
    for uid in root_el.findall("uniqueid"):
        uid_type = (uid.get("type") or "").lower()
        value = (uid.text or "").strip()
        if not value:
            continue
        if uid_type == "tmdb" and not tmdb_id:
            tmdb_id = value
        elif uid_type in ("bgm", "bangumi") and not bgm_id:
            bgm_id = value
    if not tmdb_id:
        el = root_el.find("tmdbid")
        if el is not None and (el.text or "").strip():
            tmdb_id = el.text.strip()

    tid = tmdb_id or bgm_id
    if not tid:
        return False

    use_tmdb = bool(tmdb_id)
    title_el = root_el.find("title")
    series_title = (title_el.text or "").strip() if title_el is not None else ""
    year_el = root_el.find("year")
    year = (year_el.text or "").strip() if year_el is not None else ""

    pure_name, _ = extract_lang_and_ext(
        item.old_name,
        ctx.lang_tags.get() if hasattr(ctx, "lang_tags") else "",
    )
    guessed = cached_guessit(pure_name)
    raw_s = guessed.get("season") or 1
    raw_e = extract_episode_number(pure_name, guessed)
    if raw_e is None:
        raw_e = guessed.get("episode")
    if isinstance(raw_e, list):
        raw_e = raw_e[0]
    if raw_e is None:
        return False

    season_number = int(raw_s) if str(raw_s).isdigit() else 1
    episode_number = int(raw_e)

    api_tmdb = ctx.tmdb_api_key.get().strip() if hasattr(ctx, "tmdb_api_key") else ""
    api_bgm = ctx.bgm_api_key.get().strip() if hasattr(ctx, "bgm_api_key") else ""

    episode_name, episode_plot, episode_still, season_poster = "", "", "", ""
    try:
        if use_tmdb:
            episode_name, episode_plot, episode_still = fetch_tmdb_episode_meta(
                tid,
                season_number,
                episode_number,
                api_tmdb,
                series_title,
                api_bgm,
            )
            season_poster = fetch_tmdb_season_poster(tid, season_number, api_tmdb)
        else:
            episode_name, episode_plot, episode_still, season_poster = fetch_hybrid_episode_meta(
                series_title,
                tid,
                season_number,
                episode_number,
                api_bgm,
                api_tmdb,
                year,
            )
    except Exception:
        return False

    _, ext_full = extract_lang_and_ext(
        item.old_name,
        ctx.lang_tags.get() if hasattr(ctx, "lang_tags") else "",
    )
    season_fmt = f"{season_number:02d}"
    episode_fmt = f"{episode_number:02d}"
    ep_range = extract_episode_range(pure_name, guessed)
    episode_end = None
    if ep_range and ep_range[0] == episode_number and ep_range[1] > episode_number:
        episode_end = ep_range[1]
        episode_fmt = f"{episode_number:02d}-E{episode_end:02d}"
    safe_title = safe_filename(series_title)
    fallback_ep_text = (
        f"第 {episode_number}-{episode_end} 集" if episode_end else f"第 {episode_number} 集"
    )
    safe_ep = safe_filename(episode_name or fallback_ep_text)

    new_fn, media_suffix = ctx._render_media_filename(
        ctx.tv_format.get(),
        title=safe_title,
        year=year,
        season=season_fmt,
        episode=episode_fmt,
        ep_name=safe_ep,
        ext=ext_full,
        source_filename=item.old_name,
        pure_name=pure_name,
        source_provider="tmdb" if use_tmdb else "bgm",
        media_id=tid,
        is_tv=True,
    )

    item.metadata = {
        "id": tid,
        "provider": "tmdb" if use_tmdb else "bgm",
        "title": series_title,
        "year": year,
        "ep_title": episode_name or fallback_ep_text,
        "overview": "",
        "ep_plot": episode_plot,
        "s": season_number,
        "e": episode_number,
        "e_end": episode_end,
        "poster": None,
        "fanart": None,
        "still": episode_still,
        "s_poster": season_poster,
        "type": "episode",
        "actors": [],
        "directors": [],
        "genres": [],
        "studios": [],
        "runtime": None,
        "status": "",
        "rating": 0,
        "votes": 0,
        "release": "",
        "original_title": "",
        "media_suffix": media_suffix,
    }
    recognition_result = RecognitionResult(
        title=series_title,
        year=year,
        media_type="episode",
        season=season_number,
        episode=episode_number,
        episode_end=episode_end,
        provider="tmdb" if use_tmdb else "bgm",
        provider_id=str(tid),
        parse_source="nfo_fast_path",
        query_title=series_title,
        match_reason="本地 tvshow.nfo ID 锁定",
        confidence=0.98,
        confidence_level="high",
        trace=[
            {"stage": "nfo_fast_path", "nfo": nfo_path, "matched_id": str(tid)},
            {"stage": "decision", "confidence": 0.98, "confidence_level": "high"},
        ],
    )
    item.recognition_result = recognition_result
    item.metadata["parse_source"] = recognition_result.parse_source
    item.metadata["query_title"] = recognition_result.query_title
    item.metadata["confidence"] = recognition_result.confidence
    item.metadata["confidence_level"] = recognition_result.confidence_level
    item.metadata["recognition_result"] = recognition_result.to_dict()
    item.metadata["recognition_trace"] = recognition_result.trace
    item.metadata["recognition_warnings"] = recognition_result.warnings
    item.media_suffix = media_suffix
    item.new_name_only = new_fn

    root_d = ctx.target_root.get().strip() if hasattr(ctx, "target_root") else ""
    if root_d:
        preserved_target = ""
        preserve_var = getattr(ctx, "preserve_existing_folder", None)
        if preserve_var is not None:
            getter = getattr(preserve_var, "get", None)
            preserve_enabled = bool(getter()) if callable(getter) else bool(preserve_var)
            if preserve_enabled:
                preserved_target = build_existing_library_target(item.path, new_fn, item.metadata)
        if preserved_target:
            item.full_target = preserved_target
        else:
            id_tag = f"tmdbid={tid}" if use_tmdb else f"bgmid={tid}"
            folder_name = safe_filename(f"{safe_title} [{id_tag}]")
            item.full_target = os.path.join(root_d, folder_name, f"Season {season_number}", new_fn)
    else:
        item.full_target = ""

    logger.info(
        "NFO fast-path: %s via %s tid=%s",
        os.path.basename(item.path),
        os.path.basename(nfo_path),
        tid,
    )
    return True
