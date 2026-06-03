import logging

from db.api_common import tmdb_get
from utils.app_runtime import TIMEOUT_DB_DETAIL
from utils.cache import cached_request, get_cache_key


def fetch_tmdb_credits_raw(tmdb_id, is_tv=True, api_key=""):
    if not tmdb_id or tmdb_id == "None" or not api_key.strip():
        return [], []

    stype = "tv" if is_tv else "movie"
    try:
        response = tmdb_get(
            f"https://api.themoviedb.org/3/{stype}/{tmdb_id}/credits",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()

        cast = data.get("cast") or []
        crew = data.get("crew") or []

        actors = []
        for person in cast[:20]:
            name = person.get("name") or ""
            role = person.get("character") or ""
            thumb = person.get("profile_path") or ""
            if thumb:
                thumb = f"https://image.tmdb.org/t/p/w185{thumb}"
            if name:
                actors.append({"name": name, "role": role, "thumb": thumb})

        directors = [person.get("name") for person in crew if person.get("job") == "Director" and person.get("name")]
        if is_tv and not directors:
            directors = [person.get("name") for person in (data.get("created_by") or []) if person.get("name")]

        return actors, directors
    except Exception as err:
        logging.warning("TMDB credits 获取失败 (%s): %s", tmdb_id, err)
        return [], []


def fetch_tmdb_credits(tmdb_id, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_credits_raw,
        get_cache_key("tmdb_credits_v1", f"{tmdb_id}_{is_tv}"),
        tmdb_id,
        is_tv,
        api_key,
    )
