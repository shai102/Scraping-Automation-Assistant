import difflib
import logging
import re

import requests

from db.api_common import response_body_snippet, tmdb_get
from db.bgm_api import fetch_bgm_episode
from utils.app_runtime import TIMEOUT_DB_DETAIL, TIMEOUT_DB_SEARCH
from utils.cache import cached_request, get_cache_key


def fetch_hybrid_tmdb_id_raw(title, year, api_key_tmdb):
    query = re.sub(r"(?i)HD|重制版|重製版|Remaster|Season.*|第.*季", "", title).strip()
    query_norm = re.sub(r"[\W_]+", "", str(query).lower())
    try:
        response = tmdb_get(
            "https://api.themoviedb.org/3/search/tv",
            params={"api_key": api_key_tmdb.strip(), "query": query, "language": "zh-CN"},
            timeout=TIMEOUT_DB_SEARCH,
        )
        response.raise_for_status()
        results = response.json().get("results", [])

        best_item = None
        best_score = 0.0
        for item in results:
            name = item.get("name") or item.get("original_name") or ""
            name_norm = re.sub(r"[\W_]+", "", str(name).lower())
            if not name_norm or not query_norm:
                continue
            score = difflib.SequenceMatcher(None, query_norm, name_norm).ratio()
            item_year = str(item.get("first_air_date") or "")[:4]
            if year and item_year and str(year) == item_year:
                score += 0.15
            if score > best_score:
                best_score = score
                best_item = item

        if best_item and best_score >= 0.6:
            return str(best_item["id"])
    except Exception as err:
        logging.warning("hybrid TMDB搜索失败: %s", err)
    return ""


def fetch_hybrid_tmdb_id(title, year, api_key_tmdb):
    return cached_request(
        fetch_hybrid_tmdb_id_raw,
        get_cache_key("hybrid_tmdb_id_v1", f"{title}_{year}"),
        title,
        year,
        api_key_tmdb,
    )


def fetch_hybrid_episode_meta_raw(title, subject_id, season, episode, api_key_bgm, api_key_tmdb, year=None):
    episode_name, episode_plot = fetch_bgm_episode(subject_id, season, episode, api_key_bgm)
    episode_still, season_poster = "", ""

    if api_key_tmdb and api_key_tmdb.strip():
        try:
            tmdb_id = fetch_hybrid_tmdb_id(title, year, api_key_tmdb)
            if tmdb_id:
                episode_response = tmdb_get(
                    f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season}/episode/{episode}",
                    params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"},
                    timeout=TIMEOUT_DB_DETAIL,
                )
                if episode_response.status_code == 200:
                    episode_still = episode_response.json().get("still_path", "")

                season_response = tmdb_get(
                    f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season}",
                    params={"api_key": api_key_tmdb.strip(), "language": "zh-CN"},
                    timeout=TIMEOUT_DB_DETAIL,
                )
                if season_response.status_code == 200:
                    season_poster = season_response.json().get("poster_path", "")
        except Exception as err:
            snippet = ""
            if isinstance(err, requests.exceptions.HTTPError):
                snippet = response_body_snippet(getattr(err, "response", None))
            else:
                snippet = response_body_snippet(locals().get("response"))
            if snippet:
                logging.warning("混合来源补全剧集图片失败: %s，返回内容: %s", err, snippet)
            else:
                logging.warning("混合来源补全剧集图片失败: %s", err)

    return episode_name, episode_plot, episode_still, season_poster


def fetch_hybrid_episode_meta(title, subject_id, season, episode, api_key_bgm, api_key_tmdb, year=None):
    return cached_request(
        fetch_hybrid_episode_meta_raw,
        get_cache_key("hybrid_ep_v1", f"{subject_id}_{season}_{episode}"),
        title,
        subject_id,
        season,
        episode,
        api_key_bgm,
        api_key_tmdb,
        year,
    )
