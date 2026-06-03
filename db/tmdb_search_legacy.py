import difflib
import logging
import re

import requests

from db.api_common import response_body_snippet, tmdb_get
from utils.app_runtime import TIMEOUT_DB_SEARCH
from utils.title_parsing import clean_search_title, text_mentions_extra_title


def legacy_fetch_tmdb_candidates_raw_v1(title, year=None, is_tv=True, api_key=""):
    if not api_key or not api_key.strip():
        return []

    q = clean_search_title(title)
    stype = "tv" if is_tv else "movie"
    q_norm = re.sub(r"[\W_]+", "", str(q).lower())

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

    def similarity_score(item, query_norm=None):
        compare_norm = query_norm or q_norm
        name = item.get("name") or item.get("title") or ""
        original_name = item.get("original_name") or item.get("original_title") or ""
        name_norm = re.sub(r"[\W_]+", "", str(name).lower())
        original_norm = re.sub(r"[\W_]+", "", str(original_name).lower())
        scores = []
        if name_norm:
            scores.append(difflib.SequenceMatcher(None, compare_norm, name_norm).ratio())
        if original_norm:
            scores.append(difflib.SequenceMatcher(None, compare_norm, original_norm).ratio())
        return max(scores) if scores else 0.0

    def rank_results(result_sets, query):
        query_extra = text_mentions_extra_title(query)
        query_norm = re.sub(r"[\W_]+", "", str(query).lower())
        merged = {}
        merged_set_idx = {}
        order = 0
        for set_idx, results in enumerate(result_sets):
            for item in results or []:
                order += 1
                cid = str(item.get("id") or "")
                if not cid:
                    continue
                score = similarity_score(item, query_norm)
                name = item.get("name") or item.get("title") or ""
                original_name = item.get("original_name") or item.get("original_title") or ""
                name_norm = re.sub(r"[\W_]+", "", str(name).lower())
                original_norm = re.sub(r"[\W_]+", "", str(original_name).lower())
                exact = bool(query_norm and (name_norm == query_norm or original_norm == query_norm))
                extra_penalty = 1 if (not query_extra and item_extra(item)) else 0
                priority = (extra_penalty, 0 if exact else 1, -score, -float(item.get("popularity") or 0), order)
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

    try:
        search_plan = ["year", "first_air_date_year", None] if is_tv and year else ["year", None] if year else [None]
        queries = [q]
        retry_query = re.sub(r"(?i)HD|重制版|重製版|Remaster|Edition", "", q).strip()
        if retry_query and retry_query != q:
            queries.append(retry_query)

        for query in queries:
            for year_mode in search_plan:
                result_sets = [request_once(query, year_mode, "zh-CN")]
                if is_latin_query(query):
                    result_sets.append(request_once(query, year_mode, "en-US"))
                results = rank_results(result_sets, query)
                if results:
                    return items_to_candidates(results, query)

        token_queries = []
        for token in re.split(r"\s+", q):
            token = token.strip()
            if len(token) >= 4 and token.lower() != q.lower() and token not in token_queries:
                token_queries.append(token)

        fuzzy_pool = []
        seen = set()
        for token_query in token_queries:
            for item in request_once(token_query, None):
                cid = str(item.get("id") or "")
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                fuzzy_pool.append(item)

        if fuzzy_pool:
            ranked = sorted(fuzzy_pool, key=similarity_score, reverse=True)
            top = [item for item in ranked if similarity_score(item) >= 0.35]
            return items_to_candidates(top if top else ranked, " / ".join(token_queries))
        return []
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
