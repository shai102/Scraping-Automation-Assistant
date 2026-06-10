import difflib
import logging
import re

import requests

from db.api_common import response_body_snippet, tmdb_get
from db.tmdb_search_candidate_utils import (
    is_latin_query,
    items_to_candidates,
    keyword_query_variants,
    norm_text,
    quality,
    rank_results,
    similarity_score,
)
from utils.app_runtime import TIMEOUT_DB_SEARCH
from utils.title_parsing import (
    build_fallback_token_queries,
    clean_search_title,
    normalize_search_query_title,
)


def fetch_tmdb_candidates_raw(title, year=None, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return []

    title = normalize_search_query_title(title)
    q = clean_search_title(title)
    stype = "tv" if is_tv else "movie"
    raw_query = str(title or "").strip()

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

    def request_ranked(query, year_mode=None):
        result_sets = [request_once(query, year_mode, "zh-CN")]
        if is_latin_query(query):
            result_sets.append(request_once(query, year_mode, "en-US"))
        return rank_results(result_sets, query)

    def keyword_queries(base_query):
        scored = []
        seen = set()
        for keyword in request_keywords(base_query):
            name = str(keyword.get("name") or "").strip()
            key = norm_text(name)
            if not key or key in seen:
                continue
            seen.add(key)
            score = difflib.SequenceMatcher(None, norm_text(base_query), key).ratio()
            scored.append((score, len(name), name))
        scored.sort(key=lambda item: (-item[0], item[1], item[2]))
        queries = []
        seen_queries = set()
        for score, _length, name in scored[:5]:
            if score < 0.45:
                continue
            for variant in keyword_query_variants(name):
                query_text = clean_search_title(variant) or variant.strip()
                query_key = norm_text(query_text)
                if not query_key or query_key in seen_queries:
                    continue
                seen_queries.add(query_key)
                queries.append(query_text)
        return queries

    try:
        search_plan = ["year", "first_air_date_year", None] if is_tv and year else ["year", None] if year else [None]

        queries = [q]
        if raw_query and norm_text(raw_query) != norm_text(q):
            queries.append(raw_query)
        retry_query = re.sub(r"(?i)HD|重制版|重製版|重装版|Remaster|Edition", "", q).strip()
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
                    best_candidates = items_to_candidates(results, title=title, is_tv=is_tv, search_query=query)
                if current_quality[0] >= 1.0 or current_quality[2] >= 0.72:
                    return items_to_candidates(results, title=title, is_tv=is_tv, search_query=query)

        token_queries = [
            token for token in build_fallback_token_queries(q, min_length=4) if token.lower() != q.lower()
        ]
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
            ranked = sorted(fuzzy_pool, key=lambda item: similarity_score(item, norm_text(q)), reverse=True)
            top = [item for item in ranked if similarity_score(item, norm_text(q)) >= 0.35]
            chosen = top or ranked
            token_candidates = items_to_candidates(
                chosen,
                title=title,
                is_tv=is_tv,
                search_query=" / ".join(token_queries),
            )

        keyword_queries_list = []
        for query in queries:
            for keyword_query in keyword_queries(query):
                if all(norm_text(keyword_query) != norm_text(existing) for existing in keyword_queries_list):
                    keyword_queries_list.append(keyword_query)

        for keyword_query in keyword_queries_list:
            for year_mode in search_plan:
                results = request_ranked(keyword_query, year_mode)
                if not results:
                    continue
                current_quality = quality(results, keyword_query)
                if current_quality > best_quality:
                    best_quality = current_quality
                    best_candidates = items_to_candidates(
                        results,
                        title=title,
                        is_tv=is_tv,
                        search_query=keyword_query,
                    )
                if current_quality[0] >= 1.0 or current_quality[2] >= 0.72:
                    return items_to_candidates(
                        results,
                        title=title,
                        is_tv=is_tv,
                        search_query=keyword_query,
                    )

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
