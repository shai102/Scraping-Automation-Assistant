import difflib
import re

from utils.title_parsing import text_mentions_extra_title


def norm_text(text):
    return re.sub(r"[\W_]+", "", str(text or "").lower())


def items_to_candidates(items, *, title, is_tv, search_query=""):
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


def similarity_score(item, query_norm):
    name = item.get("name") or item.get("title") or ""
    original_name = item.get("original_name") or item.get("original_title") or ""
    name_norm = norm_text(name)
    original_norm = norm_text(original_name)
    scores = []
    if name_norm:
        scores.append(difflib.SequenceMatcher(None, query_norm, name_norm).ratio())
    if original_norm:
        scores.append(difflib.SequenceMatcher(None, query_norm, original_norm).ratio())
    return max(scores) if scores else 0.0


def rank_results(result_sets, query):
    query_extra = text_mentions_extra_title(query)
    query_norm = norm_text(query)
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
                    norm_text(item.get("name") or item.get("title") or "") == query_norm
                    or norm_text(item.get("original_name") or item.get("original_title") or "") == query_norm
                )
            )
            extra_penalty = 1 if (not query_extra and item_extra(item)) else 0
            priority = (
                extra_penalty,
                0 if exact else 1,
                -similarity_score(item, query_norm),
                -float(item.get("popularity") or 0),
                order,
            )
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


def quality(items, query):
    if not items:
        return (-1.0, -1.0, -1.0, -1.0)
    query_norm = norm_text(query)
    top = items[0]
    exact = 1.0 if query_norm and (
        norm_text(top.get("name") or top.get("title") or "") == query_norm
        or norm_text(top.get("original_name") or top.get("original_title") or "") == query_norm
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
