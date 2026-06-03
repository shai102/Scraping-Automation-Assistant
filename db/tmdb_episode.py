import logging
import re

import requests

from db.api_common import response_body_snippet, tmdb_get
from db.bgm_api import fetch_bgm_candidates, fetch_bgm_episode
from utils.app_runtime import TIMEOUT_DB_DETAIL
from utils.cache import cached_request, get_cache_key


def fetch_tmdb_episode_meta_raw(tv_id, season, episode, api_key, series_title="", api_key_bgm=""):
    if not tv_id or tv_id == "None" or not api_key.strip():
        return "", "", ""

    try:
        response = tmdb_get(
            f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()

        name = data.get("name")
        plot = data.get("overview")
        still = data.get("still_path", "")

        def is_placeholder(value):
            if not value:
                return True
            text = str(value).strip()
            return bool(
                re.fullmatch(r"(?i)episode\s*\d+", text)
                or re.fullmatch(r"第\s*\d+\s*[集話话]", text)
            )

        if is_placeholder(name) or not (plot or "").strip():
            response_en = tmdb_get(
                f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}",
                params={"api_key": api_key.strip(), "language": "en-US"},
                timeout=TIMEOUT_DB_DETAIL,
            )
            response_en.raise_for_status()
            data_en = response_en.json()
            en_name = data_en.get("name", "")
            if is_placeholder(name) and en_name and not is_placeholder(en_name):
                name = en_name
            if not (plot or "").strip():
                plot = data_en.get("overview", "") or plot

        placeholder_name = is_placeholder(name)
        if (not str(plot or "").strip() and (not name or placeholder_name)) and series_title:
            try:
                bgm_candidates = fetch_bgm_candidates(series_title, api_key_bgm)
                if bgm_candidates:
                    bgm_subject_id = str(bgm_candidates[0].get("id", ""))
                    if bgm_subject_id:
                        bgm_ep_name, bgm_ep_plot = fetch_bgm_episode(
                            bgm_subject_id, season, episode, api_key_bgm
                        )
                        if (not name or placeholder_name) and bgm_ep_name:
                            name = bgm_ep_name
                        if (not plot or not str(plot).strip()) and bgm_ep_plot:
                            plot = bgm_ep_plot
            except Exception as err:
                logging.warning("BGM补全剧集信息失败: %s", err)

        return name or "", plot or "", still or ""
    except Exception as err:
        snippet = ""
        if isinstance(err, requests.exceptions.HTTPError):
            snippet = response_body_snippet(getattr(err, "response", None))
        else:
            snippet = response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning("TMDb剧集详情获取失败: %s，返回内容: %s", err, snippet)
        else:
            logging.warning("TMDb剧集详情获取失败: %s", err)
        return "", "", ""


def fetch_tmdb_episode_meta(tv_id, season, episode, api_key, series_title="", api_key_bgm=""):
    key = get_cache_key("tmdb_ep_v3", f"{tv_id}_{season}_{episode}_{series_title}")
    return cached_request(
        fetch_tmdb_episode_meta_raw,
        key,
        tv_id,
        season,
        episode,
        api_key,
        series_title,
        api_key_bgm,
    )


def fetch_tmdb_season_poster_raw(tv_id, season, api_key):
    if not tv_id or tv_id == "None" or not api_key.strip():
        return ""
    try:
        response = tmdb_get(
            f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        return response.json().get("poster_path", "")
    except Exception:
        return ""


def fetch_tmdb_season_poster(tv_id, season, api_key):
    return cached_request(
        fetch_tmdb_season_poster_raw,
        get_cache_key("tmdb_season_poster", f"{tv_id}_{season}"),
        tv_id,
        season,
        api_key,
    )


def fetch_tmdb_season_episode_count_raw(tv_id, season, api_key):
    if not tv_id or tv_id == "None" or not api_key.strip():
        return 0
    try:
        response = tmdb_get(
            f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        episodes = response.json().get("episodes")
        if isinstance(episodes, list):
            return len(episodes)
        return 0
    except Exception:
        return 0


def fetch_tmdb_season_episode_count(tv_id, season, api_key):
    return cached_request(
        fetch_tmdb_season_episode_count_raw,
        get_cache_key("tmdb_season_ep_count", f"{tv_id}_{season}"),
        tv_id,
        season,
        api_key,
    )
