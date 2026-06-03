import difflib
import logging
import re

import requests

from db.api_common import bgm_throttle, response_body_snippet
from utils.app_runtime import TIMEOUT_DB_DETAIL, TIMEOUT_DB_SEARCH, USER_AGENT
from utils.cache import cached_request, get_cache_key
from utils.candidate_utils import candidate_to_result
from utils.error_utils import (
    ERROR_CODE_HTTP,
    ERROR_CODE_INVALID,
    ERROR_CODE_NO_RESULT,
    ERROR_CODE_PARSE,
    ERROR_CODE_TIMEOUT,
    ERROR_CODE_UNKNOWN,
    format_error_message,
)
from utils.proxy import session
from utils.title_parsing import (
    build_fallback_token_queries,
    clean_search_title,
    normalize_search_query_title,
)


def fetch_bgm_by_id_raw(subject_id, api_key=""):
    headers = {"User-Agent": USER_AGENT}
    if api_key and api_key.strip():
        headers["Authorization"] = f"Bearer {api_key.strip()}"

    try:
        response = session.get(
            f"https://api.bgm.tv/v0/subjects/{subject_id}",
            headers=headers,
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()
        data = response.json()

        meta = {
            "overview": data.get("summary", ""),
            "rating": data.get("rating", {}).get("score", 0),
            "poster": data.get("images", {}).get("large", ""),
            "fanart": "",
            "release": data.get("date", ""),
        }

        title = data.get("name_cn") or data.get("name") or str(subject_id)
        return title, str(data.get("id")), "ID强制锁定", meta
    except requests.exceptions.Timeout:
        return str(subject_id), "None", format_error_message(ERROR_CODE_TIMEOUT, "请求超时"), {}
    except requests.exceptions.HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            msg = format_error_message(ERROR_CODE_INVALID, "ID无效")
        else:
            msg = format_error_message(ERROR_CODE_HTTP, f"HTTP请求失败: {err}")
        snippet = response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning("BGM按ID查询HTTP失败，返回内容: %s", snippet)
        return str(subject_id), "None", msg, {}
    except ValueError:
        snippet = response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning("BGM按ID查询解析失败，返回内容: %s", snippet)
        return str(subject_id), "None", format_error_message(ERROR_CODE_PARSE, "响应解析失败"), {}
    except Exception as err:
        logging.warning("BGM按ID查询异常: %s", err)
        return str(subject_id), "None", format_error_message(ERROR_CODE_UNKNOWN, "请求异常"), {}


def fetch_bgm_by_id(subject_id, api_key=""):
    return cached_request(fetch_bgm_by_id_raw, get_cache_key("bgm_id", subject_id), subject_id, api_key)


def fetch_bgm_candidates_raw(title, year=None, api_key=""):
    title = normalize_search_query_title(title)
    query = clean_search_title(title)
    query_norm = re.sub(r"[\W_]+", "", str(query).lower())
    headers = {"User-Agent": USER_AGENT}
    if api_key and api_key.strip():
        headers["Authorization"] = f"Bearer {api_key.strip()}"

    def items_to_candidates(items):
        candidates = []
        seen_ids = set()
        for item in items[:8]:
            cid = str(item.get("id") or "")
            if not cid or cid in seen_ids:
                continue
            seen_ids.add(cid)
            release = item.get("air_date") or item.get("date") or ""
            rating = item.get("score", 0)
            meta = {
                "overview": item.get("summary", ""),
                "rating": rating,
                "poster": item.get("images", {}).get("large", ""),
                "fanart": "",
                "release": release,
            }
            candidates.append(
                {
                    "title": item.get("name_cn") or item.get("name") or title,
                    "alt_title": item.get("name") or "",
                    "id": cid,
                    "msg": "BGM候选",
                    "rating": rating,
                    "release": release,
                    "meta": meta,
                }
            )
        return candidates

    def similarity_score(item):
        name_cn = item.get("name_cn") or ""
        name = item.get("name") or ""
        name_cn_norm = re.sub(r"[\W_]+", "", str(name_cn).lower())
        name_norm = re.sub(r"[\W_]+", "", str(name).lower())
        scores = []
        if name_cn_norm:
            scores.append(difflib.SequenceMatcher(None, query_norm, name_cn_norm).ratio())
        if name_norm:
            scores.append(difflib.SequenceMatcher(None, query_norm, name_norm).ratio())
        return max(scores) if scores else 0.0

    def request_bgm(raw_query):
        bgm_throttle()
        resp = session.get(
            f"https://api.bgm.tv/search/subject/{raw_query}?type=2",
            headers=headers,
            timeout=TIMEOUT_DB_SEARCH,
        )
        resp.raise_for_status()
        return resp.json().get("list", [])

    def year_sort_key(candidate):
        if not year:
            return 1
        release = candidate.get("release") or ""
        return 0 if str(release).startswith(str(year)) else 1

    try:
        queries = [query]
        retry_query = re.sub(r"(?i)HD|重制版|重製版|Remaster|Edition", "", query).strip()
        if retry_query and retry_query != query:
            queries.append(retry_query)

        for raw_query in queries:
            results = request_bgm(raw_query)
            if results:
                candidates = items_to_candidates(results)
                candidates.sort(key=year_sort_key)
                return candidates

        token_queries = [
            token
            for token in build_fallback_token_queries(query, min_length=2)
            if token.lower() != query.lower()
        ]

        fuzzy_pool = []
        seen = set()
        for token_query in token_queries:
            for item in request_bgm(token_query):
                cid = str(item.get("id") or "")
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                fuzzy_pool.append(item)

        if fuzzy_pool:
            ranked = sorted(fuzzy_pool, key=similarity_score, reverse=True)
            top = [item for item in ranked if similarity_score(item) >= 0.35]
            pool = top if top else ranked
            candidates = items_to_candidates(pool)
            candidates.sort(key=year_sort_key)
            return candidates
        return []
    except requests.exceptions.Timeout:
        return []
    except requests.exceptions.HTTPError as err:
        snippet = response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning("BGM候选搜索HTTP失败，返回内容: %s", snippet)
        return []
    except ValueError:
        snippet = response_body_snippet(locals().get("resp"))
        if snippet:
            logging.warning("BGM候选搜索解析失败，返回内容: %s", snippet)
        return []
    except Exception:
        return []


def fetch_bgm_candidates(title, year=None, api_key=""):
    title = normalize_search_query_title(title)
    return cached_request(
        fetch_bgm_candidates_raw,
        get_cache_key("bgm_candidates_v2", f"{title}_{year}"),
        title,
        year,
        api_key,
    )


def fetch_bgm_info_raw(title, api_key=""):
    candidates = fetch_bgm_candidates_raw(title, api_key=api_key)
    if candidates:
        return candidate_to_result(candidates[0], "BGM命中")
    return title, "None", format_error_message(ERROR_CODE_NO_RESULT, "BGM无结果"), {}


def fetch_bgm_info(title, api_key=""):
    return cached_request(fetch_bgm_info_raw, get_cache_key("bgm_search", title), title, api_key)


def fetch_bgm_episode_raw(subject_id, season, episode, api_key_bgm):
    headers = {"User-Agent": USER_AGENT}
    if api_key_bgm and api_key_bgm.strip():
        headers["Authorization"] = f"Bearer {api_key_bgm.strip()}"

    try:
        response = session.get(
            f"https://api.bgm.tv/v0/episodes?subject_id={subject_id}&type=0&limit=100",
            headers=headers,
            timeout=TIMEOUT_DB_DETAIL,
        )
        response.raise_for_status()

        for ep in response.json().get("data", []):
            if ep.get("sort") == episode:
                return ep.get("name_cn") or ep.get("name") or "", ep.get("desc", "")
    except Exception:
        pass

    return "", ""


def fetch_bgm_episode(subject_id, season, episode, api_key_bgm):
    return cached_request(
        fetch_bgm_episode_raw,
        get_cache_key("bgm_ep", f"{subject_id}_{season}_{episode}"),
        subject_id,
        season,
        episode,
        api_key_bgm,
    )
