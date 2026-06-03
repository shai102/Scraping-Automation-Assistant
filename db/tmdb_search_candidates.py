from db.tmdb_search_candidates_core import fetch_tmdb_candidates_raw
from utils.cache import cached_request, get_cache_key
from utils.candidate_utils import candidate_to_result
from utils.error_utils import ERROR_CODE_CONFIG, ERROR_CODE_NO_RESULT, format_error_message
from utils.title_parsing import normalize_search_query_title


def fetch_tmdb_candidates(title, year=None, is_tv=True, api_key=""):
    title = normalize_search_query_title(title)
    return cached_request(
        fetch_tmdb_candidates_raw,
        get_cache_key("tmdb_candidates_v6", f"{title}_{year}_{is_tv}"),
        title,
        year,
        is_tv,
        api_key,
    )


def fetch_tmdb_info_raw(title, year=None, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return title, "None", format_error_message(ERROR_CODE_CONFIG, "未配置TMDb Key"), {}

    candidates = fetch_tmdb_candidates_raw(title, year, is_tv, api_key)
    if candidates:
        return candidate_to_result(candidates[0], "TMDb命中")
    return title, "None", format_error_message(ERROR_CODE_NO_RESULT, "TMDb无结果"), {}


def fetch_tmdb_info(title, year=None, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_info_raw,
        get_cache_key("tmdb_search_v4", f"{title}_{year}_{is_tv}"),
        title,
        year,
        is_tv,
        api_key,
    )
