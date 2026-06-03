import logging
import os
import re
import threading

from guessit import guessit

from ai.ollama_ai import is_ai_rate_limited_error
from core.recognition.preview_helpers import (
    PROLOGUE_RE,
    SPECIAL_EPISODE_RE,
    SPECIAL_TAG_RE,
    cache_reuse_status as _cache_reuse_status,
    can_reuse_same_folder_season_cache as _can_reuse_same_folder_season_cache,
    derive_guessit_fields as _derive_guessit_fields,
    dir_cache_key as _dir_cache_key,
    extract_zero_episode_special_slot as _extract_zero_episode_special_slot,
    fetch_ai_parse as _fetch_ai_parse,
    guessit_needs_assist as _guessit_needs_assist,
    is_decimal_recap_episode as _is_decimal_recap_episode,
    mark_ai_rate_limited as _mark_ai_rate_limited,
    mark_skipped_recap as _mark_skipped_recap,
    merge_assist_parse as _merge_assist_parse,
    release_dir_parse_event as _release_dir_parse_event,
    season_value_or_default as _season_value_or_default,
    store_dir_parse_cache as _store_dir_parse_cache,
)
from utils.library_paths import extract_db_id_from_path
from utils.title_parsing import derive_title_from_filename, extract_episode_number
from utils.value_utils import safe_int


logger = logging.getLogger(__name__)


def recognize_preview_item(gui, item):
    pure, ext = gui.extract_lang_and_ext(item.old_name)
    dir_p = item.dir
    mode = gui.source_var.get()

    strip_kw = getattr(gui, "strip_keywords", None) or []
    pure_for_parse = pure
    if strip_kw:
        for kw in strip_kw:
            if kw:
                pure_for_parse = re.sub(
                    re.escape(kw), " ", pure_for_parse, flags=re.IGNORECASE
                )
        pure_for_parse = re.sub(r"\s+", " ", pure_for_parse).strip()

    g = guessit(pure_for_parse)

    extracted_ep = extract_episode_number(pure, g)
    guess_title, guess_year, guess_season, guess_episode = _derive_guessit_fields(
        gui, pure, dir_p, g, extracted_ep
    )
    guessit_needs_assist = _guessit_needs_assist(
        pure, dir_p, g, guess_title, extracted_ep
    )
    guessit_confident = not guessit_needs_assist
    cache_title_aliases = [
        guess_title,
        derive_title_from_filename(pure),
        os.path.basename(os.path.dirname(dir_p or "")),
    ]
    folder_id_title_hints = [
        guess_title,
        derive_title_from_filename(pure),
        os.path.basename(os.path.dirname(dir_p or "")),
        os.path.basename(dir_p or ""),
    ]

    ai_mode_obj = getattr(gui, "ai_mode", None)
    ai_mode_val = ai_mode_obj.get() if ai_mode_obj else "assist"

    mode = gui.source_var.get() if hasattr(gui, "source_var") else "siliconflow_tmdb"
    has_folder_id = bool(
        extract_db_id_from_path(item.path, mode, folder_id_title_hints)
    )

    dir_cache_key = _dir_cache_key(dir_p, guess_season)
    dir_parse_event = None
    is_parse_resolver = False
    with gui.cache_lock:
        cached_ai = gui.dir_cache.get(dir_cache_key)
        if not cached_ai:
            if not hasattr(gui, "dir_parse_events"):
                gui.dir_parse_events = {}
            dir_parse_event = gui.dir_parse_events.get(dir_cache_key)
            if dir_parse_event is None:
                dir_parse_event = threading.Event()
                gui.dir_parse_events[dir_cache_key] = dir_parse_event
                is_parse_resolver = True

    try:
        if not cached_ai and dir_parse_event and not is_parse_resolver:
            if not dir_parse_event.wait(timeout=120):
                logger.warning("等待同目录解析缓存超时，将单独识别当前文件")
            with gui.cache_lock:
                cached_ai = gui.dir_cache.get(dir_cache_key)

        parse_source = "guessit"

        if cached_ai and (
            _can_reuse_same_folder_season_cache(cached_ai, guess_season, g)
            or gui._can_reuse_dir_ai(cached_ai, pure, g)
        ):
            t = cached_ai["title"]
            y = cached_ai.get("year")
            s = gui._pick_season(
                pure,
                g,
                _season_value_or_default(cached_ai.get("season"), 1),
            )
            e = extracted_ep if extracted_ep is not None else guess_episode
            ai_data = cached_ai
            parse_source = cached_ai.get("parse_source", "guessit")
            ai_msg = _cache_reuse_status(parse_source)
        else:
            ai_data = None
            ai_parse_succeeded = False
            t = guess_title
            y = guess_year
            s = guess_season
            e = guess_episode
            ai_msg = "猜测"

            if ai_mode_val == "force":
                ai_data, ai_msg = _fetch_ai_parse(gui, pure_for_parse)
                if ai_data:
                    ai_parse_succeeded = True
                    t = ai_data.get("title", "未知")
                    y = ai_data.get("year")
                    ai_season = _season_value_or_default(ai_data.get("season"), 1)
                    s = gui._pick_season(pure, g, ai_season)
                    e = (
                        extracted_ep
                        or extract_episode_number(pure, None, ai_data)
                        or safe_int(ai_data.get("episode"), 1)
                    )
                    parse_source = "ai"
                    with gui.cache_lock:
                        _store_dir_parse_cache(
                            gui,
                            dir_cache_key,
                            ai_data,
                            t,
                            y,
                            s,
                            e,
                            "ai",
                            cache_title_aliases,
                            cache_season=guess_season,
                        )
                else:
                    if is_ai_rate_limited_error(ai_msg):
                        _mark_ai_rate_limited(item)
                        return None
                    item.metadata = {"id": "None", "parse_source": "ai"}
                    item.new_name_only = ""
                    item.full_target = ""
                    item.parse_source = "ai"
                    return None
            else:
                if ai_mode_val == "assist" and guessit_needs_assist and not has_folder_id:
                    ai_data, ai_msg = _fetch_ai_parse(gui, pure_for_parse)
                    if not ai_data and is_ai_rate_limited_error(ai_msg):
                        _mark_ai_rate_limited(item)
                        return None
                    if ai_data:
                        ai_parse_succeeded = True
                        t, y, s, e, parse_source = _merge_assist_parse(
                            gui,
                            pure,
                            dir_p,
                            g,
                            guess_title,
                            guess_year,
                            guess_season,
                            guess_episode,
                            extracted_ep,
                            ai_data,
                        )
                        if parse_source == "hybrid":
                            ai_msg = "AI辅助"
                        elif parse_source == "ai":
                            ai_msg = "AI识别"
                if parse_source == "guessit" and (guessit_confident or ai_parse_succeeded):
                    with gui.cache_lock:
                        if dir_cache_key not in gui.dir_cache:
                            _store_dir_parse_cache(
                                gui,
                                dir_cache_key,
                                None,
                                t,
                                y,
                                s,
                                e,
                                "guessit",
                                cache_title_aliases,
                                cache_season=guess_season,
                            )
                elif parse_source != "guessit":
                    with gui.cache_lock:
                        _store_dir_parse_cache(
                            gui,
                            dir_cache_key,
                            ai_data,
                            t,
                            y,
                            s,
                            e,
                            parse_source,
                            cache_title_aliases,
                            cache_season=guess_season,
                        )
    finally:
        _release_dir_parse_event(
            gui, dir_cache_key, dir_parse_event if is_parse_resolver else None
        )

    if SPECIAL_TAG_RE.search(pure):
        explicit_s_in_name = gui._extract_explicit_season(pure)
        if explicit_s_in_name is None:
            s = 0
            sp_match = SPECIAL_EPISODE_RE.search(pure)
            if sp_match:
                e = int(sp_match.group(1))
            elif PROLOGUE_RE.search(pure):
                e = 0

    recap_status = ""
    zero_episode_special = _extract_zero_episode_special_slot(pure)
    if zero_episode_special is not None:
        s = 0
        e = zero_episode_special
        e_calc = zero_episode_special
        recap_status = "总集篇归入S00"
    elif _is_decimal_recap_episode(pure):
        _mark_skipped_recap(gui, item, t, "decimal_recap")
        return None
    else:
        e_calc = e

    media_type = gui._resolve_media_type(g, pure_name=pure, extracted_ep=extracted_ep)
    is_tv = media_type == "episode"
    path_key = item.path

    forced_s = gui.forced_seasons.get(path_key)
    if forced_s is not None:
        s = forced_s

    forced_o = gui.forced_offsets.get(path_key, 0)
    if isinstance(e, list):
        e = e[0]
        e_calc = e

    if forced_o != 0:
        e_calc = max(1, safe_int(e, 1) + forced_o)

    return {
        "pure": pure,
        "ext": ext,
        "dir_p": dir_p,
        "mode": mode,
        "pure_for_parse": pure_for_parse,
        "g": g,
        "extracted_ep": extracted_ep,
        "guess_title": guess_title,
        "guess_year": guess_year,
        "guess_season": guess_season,
        "guess_episode": guess_episode,
        "cache_title_aliases": cache_title_aliases,
        "ai_mode_val": ai_mode_val,
        "dir_cache_key": dir_cache_key,
        "ai_data": ai_data,
        "ai_msg": ai_msg,
        "parse_source": parse_source,
        "title": t,
        "year": y,
        "season": s,
        "episode": e,
        "episode_calc": e_calc,
        "recap_status": recap_status,
        "media_type": media_type,
        "is_tv": is_tv,
        "path_key": path_key,
    }
