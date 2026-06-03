import os

from utils.value_utils import normalize_compare_text, safe_int, safe_str


def is_ai_rate_limited_item(item):
    status_text = str(getattr(item, "status_text", "") or "")
    return "AI限流" in status_text and str((getattr(item, "metadata", {}) or {}).get("id") or "None") == "None"


def cache_reuse_status(parse_source):
    source = str(parse_source or "guessit").strip().lower()
    if source == "ai":
        return "AI复用"
    if source == "hybrid":
        return "AI辅助复用"
    return "guessit复用"


def retry_rate_limited_siblings(gui, current_index, dir_p):
    retry_indices = []
    with gui.cache_lock:
        inflight = getattr(gui, "ai_retry_inflight", None)
        if inflight is None:
            inflight = set()
            gui.ai_retry_inflight = inflight

        for idx, other in enumerate(gui.file_list):
            if idx == current_index or other.dir != dir_p:
                continue
            if not is_ai_rate_limited_item(other):
                continue
            if other.id in inflight:
                continue
            inflight.add(other.id)
            retry_indices.append((idx, other.id))

    for idx, item_id in retry_indices:
        try:
            gui.process_task(idx, advance_progress=False)
        finally:
            with gui.cache_lock:
                gui.ai_retry_inflight.discard(item_id)


def collect_cache_title_aliases(primary_title, aliases=None):
    seen = set()
    values = []
    for raw in [primary_title, *(aliases or [])]:
        text = str(raw or "").strip()
        key = normalize_compare_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        values.append(text)
    return values


def build_dir_cache_entry(ai_data, title, year, season, episode, parse_source, aliases=None):
    cache_data = dict(ai_data or {})
    cache_data.update(
        {
            "title": title,
            "year": year,
            "season": season,
            "episode": episode,
            "parse_source": parse_source,
            "title_aliases": collect_cache_title_aliases(title, aliases),
        }
    )
    return cache_data


def dir_cache_key(dir_path, season):
    dir_key = os.path.normcase(os.path.normpath(str(dir_path or "")))
    return f"{dir_key}||season={safe_int(season, 1)}"


def can_reuse_same_folder_season_cache(cached_ai, current_season, guess_data=None):
    if not isinstance(cached_ai, dict):
        return False
    cached_season = safe_int(
        cached_ai.get("cache_season", cached_ai.get("season")),
        safe_int(current_season, 1),
    )
    if cached_season != safe_int(current_season, 1):
        return False

    cached_year = safe_str(cached_ai.get("year"))
    guess_year = safe_str((guess_data or {}).get("year"))
    if cached_year and guess_year and cached_year != guess_year:
        return False
    return True


def store_dir_parse_cache(
    gui,
    cache_key,
    ai_data,
    title,
    year,
    season,
    episode,
    parse_source,
    aliases=None,
    cache_season=None,
):
    cache_entry = build_dir_cache_entry(ai_data, title, year, season, episode, parse_source, aliases)
    cache_entry["cache_season"] = safe_int(
        season if cache_season is None else cache_season,
        safe_int(season, 1),
    )
    gui.dir_cache[cache_key] = cache_entry
    return cache_entry


def release_dir_parse_event(gui, cache_key, event):
    if not event:
        return
    with gui.cache_lock:
        events = getattr(gui, "dir_parse_events", {})
        if events.get(cache_key) is event:
            events.pop(cache_key, None)
    event.set()


def mark_ai_rate_limited(item):
    item.metadata = {
        "id": "None",
        "parse_source": "ai",
        "error_code": "rate_limited",
        "error_msg": "AI接口限流，请稍后重试",
    }
    item.parse_source = "ai"
    item.new_name_only = ""
    item.full_target = ""
