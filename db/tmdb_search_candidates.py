import difflib
import logging
import re

import requests

from db.api_common import response_body_snippet, tmdb_get
from utils.app_runtime import TIMEOUT_DB_SEARCH
from utils.cache import cached_request, get_cache_key
from utils.candidate_utils import candidate_to_result
from utils.error_utils import ERROR_CODE_CONFIG, ERROR_CODE_NO_RESULT, format_error_message
from utils.title_parsing import (
    build_fallback_token_queries,
    clean_search_title,
    normalize_search_query_title,
    text_mentions_extra_title,
)


def fetch_tmdb_candidates_raw(title, year=None, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return []

    title = normalize_search_query_title(title)
    q = clean_search_title(title)
    stype = "tv" if is_tv else "movie"
    raw_query = str(title or "").strip()

    def norm(text):
        return re.sub(r"[\W_]+", "", str(text or "").lower())

    q_norm = norm(q)

    def items_to_candidates(items, search_query=""):
        candidates = []
        seen_ids = set()
        for rank, item in enumerate(items[:8], 1):
            cid = str(item.get("id") or "")
            if not cid or cid in seen_ids:
                continue
            seen_ids.add(cid)

            release = item.get("first_air_date") or item.get("release_date") or ""
            rating = item.get("vote_average", 0)
            meta = {
                "overview": item.get("overview", ""),
                "rating": rating,
                "popularity": item.get("popularity", 0),
                "poster": item.get("poster_path", ""),
                "fanart": item.get("backdrop_path", ""),
                "release": release,
                "original_title": item.get("original_name") or item.get("original_title") or "",
                "search_query": search_query,
                "search_rank": rank,
            }
            candidates.append(
                {
                    "title": item.get("name") or item.get("title") or title,
                    "alt_title": item.get("original_name") or item.get("original_title") or "",
                    "id": cid,
                    "msg": f"TMDb{'剧集' if is_tv else '电影'}候选",
                    "rating": rating,
                    "release": release,
                    "meta": meta,
                }
            )
        return candidates

    def is_latin_query(query):
        text = str(query or "")
        return bool(re.search(r"[A-Za-z]", text)) and not bool(re.search(r"[\u4e00-\u9fff]", text))

    def item_extra(item):
        fields = [
            item.get("name") or item.get("title") or "",
            item.get("original_name") or item.get("original_title") or "",
        ]
        return text_mentions_extra_title(" ".join(str(v) for v in fields if v))

    def request_once(query, year_mode=None, language="zh-CN"):
        params = {"api_key": api_key.strip(), "query": query, "language": language}
        if year:
            if year_mode == "year":
                params["year"] = year
            elif year_mode == "first_air_date_year":
                params["first_air_date_year"] = year
        response = tmdb_get(
            f"https://api.themoviedb.org/3/search/{stype}",
            params=params,
            timeout=TIMEOUT_DB_SEARCH,
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def request_keywords(query):
        response = tmdb_get(
            "https://api.themoviedb.org/3/search/keyword",
            params={"api_key": api_key.strip(), "query": query},
            timeout=TIMEOUT_DB_SEARCH,
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def similarity_score(item, query_norm=None):
        compare_norm = query_norm or q_norm
        name = item.get("name") or item.get("title") or ""
        original_name = item.get("original_name") or item.get("original_title") or ""
        name_norm = norm(name)
        original_norm = norm(original_name)
        scores = []
        if name_norm:
            scores.append(difflib.SequenceMatcher(None, compare_norm, name_norm).ratio())
        if original_norm:
            scores.append(difflib.SequenceMatcher(None, compare_norm, original_norm).ratio())
        return max(scores) if scores else 0.0

    def rank_results(result_sets, query):
        query_extra = text_mentions_extra_title(query)
        query_norm = norm(query)
        merged = {}
        merged_set_idx = {}
        order = 0
        for set_idx, results in enumerate(result_sets):
            for item in results or []:
                order += 1
                cid = str(item.get("id") or "")
                if not cid:
                    continue
                exact = bool(
                    query_norm
                    and (
                        norm(item.get("name") or item.get("title") or "") == query_norm
                        or norm(item.get("original_name") or item.get("original_title") or "") == query_norm
                    )
                )
                extra_penalty = 1 if (not query_extra and item_extra(item)) else 0
                priority = (extra_penalty, 0 if exact else 1, -similarity_score(item, query_norm), -float(item.get("popularity") or 0), order)
                previous = merged.get(cid)
                if previous is None:
                    merged[cid] = (priority, item)
                    merged_set_idx[cid] = set_idx
                elif set_idx < merged_set_idx[cid]:
                    merged[cid] = (min(priority, previous[0]), item)
                    merged_set_idx[cid] = set_idx
                elif set_idx == merged_set_idx[cid] and priority < previous[0]:
                    merged[cid] = (priority, item)
                elif set_idx > merged_set_idx[cid] and priority < previous[0]:
                    merged[cid] = (priority, previous[1])
        return [item for _, item in sorted(merged.values(), key=lambda pair: pair[0])]

    def request_ranked(query, year_mode=None):
        result_sets = [request_once(query, year_mode, "zh-CN")]
        if is_latin_query(query):
            result_sets.append(request_once(query, year_mode, "en-US"))
        return rank_results(result_sets, query)

    def quality(items, query):
        if not items:
            return (-1.0, -1.0, -1.0, -1.0)
        query_norm = norm(query)
        top = items[0]
        exact = 1.0 if query_norm and (
            norm(top.get("name") or top.get("title") or "") == query_norm
            or norm(top.get("original_name") or top.get("original_title") or "") == query_norm
        ) else 0.0
        extra_penalty = 0.0 if text_mentions_extra_title(query) or not item_extra(top) else -1.0
        top_score = similarity_score(top, query_norm)
        popularity = float(top.get("popularity") or 0.0)
        return (exact, extra_penalty, top_score, popularity)

    def keyword_query_variants(keyword_name):
        variants = []
        text = str(keyword_name or "").strip()
        if not text:
            return variants
        variants.append(text)
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9 '&:!+\\-]*", text):
            title_case = " ".join(part[:1].upper() + part[1:] if part else part for part in text.split())
            if title_case not in variants:
                variants.append(title_case)
        return variants

    def keyword_queries(base_query):
        scored = []
        seen = set()
        for keyword in request_keywords(base_query):
            name = str(keyword.get("name") or "").strip()
            key = norm(name)
            if not key or key in seen:
                continue
            seen.add(key)
            score = difflib.SequenceMatcher(None, norm(base_query), key).ratio()
            scored.append((score, len(name), name))
        scored.sort(key=lambda item: (-item[0], item[1], item[2]))
        queries = []
        seen_queries = set()
        for score, _length, name in scored[:5]:
            if score < 0.45:
                continue
            for variant in keyword_query_variants(name):
                query_text = clean_search_title(variant) or variant.strip()
                query_key = norm(query_text)
                if not query_key or query_key in seen_queries:
                    continue
                seen_queries.add(query_key)
                queries.append(query_text)
        return queries

    try:
        search_plan = ["year", "first_air_date_year", None] if is_tv and year else ["year", None] if year else [None]

        queries = [q]
        if raw_query and norm(raw_query) != norm(q):
            queries.append(raw_query)
        retry_query = re.sub(r"(?i)HD|閲嶅埗鐗坾閲嶈＝鐗坾Remaster|Edition", "", q).strip()
        if retry_query and retry_query != q:
            queries.append(retry_query)

        best_quality = (-1.0, -1.0, -1.0, -1.0)
        best_candidates = []

        for query in queries:
            for year_mode in search_plan:
                results = request_ranked(query, year_mode)
                if not results:
                    continue
                current_quality = quality(results, query)
                if current_quality > best_quality:
                    best_quality = current_quality
                    best_candidates = items_to_candidates(results, query)
                if current_quality[0] >= 1.0 or current_quality[2] >= 0.72:
                    return items_to_candidates(results, query)

        token_queries = [token for token in build_fallback_token_queries(q, min_length=4) if token.lower() != q.lower()]
        token_candidates = []
        fuzzy_pool = []
        seen = set()
        for token_query in token_queries:
            for item in request_ranked(token_query, None):
                cid = str(item.get("id") or "")
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                fuzzy_pool.append(item)

        if fuzzy_pool:
            ranked = sorted(fuzzy_pool, key=similarity_score, reverse=True)
            top = [item for item in ranked if similarity_score(item) >= 0.35]
            chosen = top or ranked
            token_candidates = items_to_candidates(chosen, " / ".join(token_queries))

        keyword_queries_list = []
        for query in queries:
            for keyword_query in keyword_queries(query):
                if all(norm(keyword_query) != norm(existing) for existing in keyword_queries_list):
                    keyword_queries_list.append(keyword_query)

        for keyword_query in keyword_queries_list:
            for year_mode in search_plan:
                results = request_ranked(keyword_query, year_mode)
                if not results:
                    continue
                current_quality = quality(results, keyword_query)
                if current_quality > best_quality:
                    best_quality = current_quality
                    best_candidates = items_to_candidates(results, keyword_query)
                if current_quality[0] >= 1.0 or current_quality[2] >= 0.72:
                    return items_to_candidates(results, keyword_query)

        return best_candidates or token_candidates
    except requests.exceptions.Timeout:
        return []
    except requests.exceptions.HTTPError as err:
        snippet = response_body_snippet(getattr(err, "response", None))
        if snippet:
            logging.warning("TMDb搜索HTTP失败，返回内容: %s", snippet)
        return []
    except ValueError:
        snippet = response_body_snippet(locals().get("response"))
        if snippet:
            logging.warning("TMDb搜索解析失败，返回内容: %s", snippet)
        return []
    except Exception as err:
        logging.error("TMDb搜索失败: %s", err)
        return []


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
