import logging

import requests

from db.api_common import response_body_snippet, tmdb_get
from utils.app_runtime import TIMEOUT_DB_DETAIL
from utils.cache import cached_request, get_cache_key
from utils.error_utils import (
    ERROR_CODE_CONFIG,
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    format_error_message,
)


def fetch_tmdb_by_id_raw(tmdb_id, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return str(tmdb_id), "None", format_error_message(ERROR_CODE_CONFIG, "未配置TMDb Key"), {}

    stype = "tv" if is_tv else "movie"

    try:
        response = tmdb_get(
            f"https://api.themoviedb.org/3/{stype}/{tmdb_id}",
            params={"api_key": api_key.strip(), "language": "zh-CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()

        meta = {
            "overview": data.get("overview", ""),
            "rating": data.get("vote_average", 0),
            "votes": data.get("vote_count", 0),
            "poster": data.get("poster_path", ""),
            "fanart": data.get("backdrop_path", ""),
            "release": data.get("first_air_date") or data.get("release_date") or "",
            "original_title": data.get("original_name") or data.get("original_title") or "",
            "genres": [g["name"] for g in (data.get("genres") or []) if g.get("name")],
            "studios": [
                n["name"]
                for n in (data.get("networks") or data.get("production_companies") or [])
                if n.get("name")
            ],
            "runtime": (data.get("episode_run_time") or [None])[0] if is_tv else data.get("runtime"),
            "status": data.get("status", ""),
        }

        if not meta["overview"]:
            try:
                response_en = tmdb_get(
                    f"https://api.themoviedb.org/3/{stype}/{tmdb_id}",
                    params={"api_key": api_key.strip(), "language": "en-US"},
                    timeout=TIMEOUT_DB_DETAIL,
                )
                if response_en.status_code == 200:
                    meta["overview"] = response_en.json().get("overview", "")
            except Exception:
                pass

        title = data.get("name") or data.get("title") or str(tmdb_id)
        return title, str(data.get("id")), "ID锁定成功", meta
    except requests.exceptions.Timeout:
        return str(tmdb_id), "None", format_error_message(ERROR_CODE_TIMEOUT, "请求超时"), {}
    except requests.exceptions.HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            msg = format_error_message(ERROR_CODE_INVALID, "ID无效")
        else:
            msg = format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
        snippet = response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning("TMDb按ID查询HTTP失败，返回内容: %s", snippet)
        return str(tmdb_id), "None", msg, {}
    except ValueError:
        snippet = response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning("TMDb按ID查询解析失败，返回内容: %s", snippet)
        return str(tmdb_id), "None", format_error_message(ERROR_CODE_PARSE, "响应解析失败"), {}
    except Exception as err:
        logging.warning("TMDb按ID查询异常: %s", err)
        return str(tmdb_id), "None", format_error_message(ERROR_CODE_UNKNOWN, "请求异常"), {}


def fetch_tmdb_by_id(tmdb_id, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_by_id_raw,
        get_cache_key("tmdb_id", f"{tmdb_id}_{is_tv}"),
        tmdb_id,
        is_tv,
        api_key,
    )


def fetch_tmdb_zh_alternative_title_raw(tmdb_id, is_tv=True, api_key=""):
    """从 TMDB 别名接口获取中文标题（zh-CN 详情标题缺失时的兜底）。"""
    if not api_key or not api_key.strip():
        return ""
    stype = "tv" if is_tv else "movie"
    try:
        response = tmdb_get(
            f"https://api.themoviedb.org/3/{stype}/{tmdb_id}/alternative_titles",
            params={"api_key": api_key.strip(), "country": "CN"},
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()
        # 电影返回 titles，剧集返回 results
        entries = data.get("titles") or data.get("results") or []
        import re as _re

        for entry in entries:
            if str(entry.get("iso_3166_1") or "").upper() not in ("CN", "HK", "TW", "SG"):
                continue
            title = str(entry.get("title") or "").strip()
            if title and _re.search(r"[一-鿿]", title):
                return title
    except Exception as err:
        logging.warning("TMDb别名查询失败 %s/%s: %s", stype, tmdb_id, err)
    return ""


def fetch_tmdb_zh_alternative_title(tmdb_id, is_tv=True, api_key=""):
    return cached_request(
        fetch_tmdb_zh_alternative_title_raw,
        get_cache_key("tmdb_alt_zh_v1", f"{tmdb_id}_{is_tv}"),
        tmdb_id,
        is_tv,
        api_key,
    )
