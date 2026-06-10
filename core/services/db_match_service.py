import difflib
import re

from db.tmdb_api import (
    fetch_bgm_candidates,
    fetch_tmdb_candidates,
)
from utils.title_parsing import (
    GROUP_RELEASE_BRACKET_RE,
    build_db_query_plan,
    candidate_looks_like_extra_title,
    candidate_looks_like_unrequested_variant,
    derive_title_from_filename,
    text_mentions_extra_title,
)
from utils.candidate_utils import candidate_to_result
from utils.value_utils import (
    extract_year_from_release,
    normalize_compare_text,
    safe_str,
    years_within_tolerance,
)


def _has_cjk(text):
    return bool(re.search(r"[\u4e00-\u9fff\u3040-\u30ff]", str(text or "")))


def _is_substantial_query_norm(norm):
    """Decide whether a normalized query title is long enough to trust a direct hit.

    Latin titles need >=6 chars to avoid spurious matches, but CJK titles are
    inherently short (e.g. 咒术回战 -> 4 chars) and would otherwise never qualify.
    """
    if not norm:
        return False
    if _has_cjk(norm):
        return len(norm) >= 2
    return len(norm) >= 6


def pick_strong_tmdb_direct_hit(query_titles, year, candidates):
    """Trust a direct TMDb rank-1 hit before semantic rerank/model override."""
    requested_year = safe_str(year).strip()
    preferred_norms = []
    seen_norms = set()
    for raw in query_titles or []:
        text = str(raw or "").strip()
        norm = normalize_compare_text(text)
        if not _is_substantial_query_norm(norm) or norm in seen_norms:
            continue
        seen_norms.add(norm)
        preferred_norms.append(norm)

    if not preferred_norms:
        return None, ""

    grouped = {}
    for candidate in candidates or []:
        meta = candidate.get("meta") or {}
        search_query = str(meta.get("search_query") or "").strip()
        search_norm = normalize_compare_text(search_query)
        if not search_norm:
            continue
        grouped.setdefault(search_norm, []).append(candidate)

    for norm in preferred_norms:
        hits = grouped.get(norm) or []
        if not hits:
            continue

        hits = sorted(
            hits,
            key=lambda cand: (
                int((cand.get("meta") or {}).get("search_rank") or 999),
                -float(cand.get("rating") or 0),
            ),
        )
        top = hits[0]
        top_meta = top.get("meta") or {}
        top_rank = int(top_meta.get("search_rank") or 999)
        top_year = extract_year_from_release(top.get("release") or "")
        year_ok = years_within_tolerance(requested_year, top_year)
        if top_rank == 1 and year_ok:
            return top, str(top_meta.get("search_query") or "")

    return None, ""


def resolve_db_match(ctx, item, query_title, year, is_tv, mode, ai_data, guessed):
    source_name = "TMDb" if mode == "siliconflow_tmdb" else "BGM"
    db_year = None if is_tv else year
    query_groups = build_db_query_plan(item, query_title, ai_data, guessed)
    merged, seen_ids = [], set()
    used_query, first_hit = query_title, False

    def search_queries(query_titles, fetch_func, limit=10):
        nonlocal used_query, first_hit
        found = []
        for query in query_titles:
            current = fetch_func(query)
            if not current:
                continue
            if not first_hit:
                used_query = query
                first_hit = True
            for candidate in current:
                candidate_id = str(candidate.get("id") or "")
                if not candidate_id or candidate_id in seen_ids:
                    continue
                seen_ids.add(candidate_id)
                found.append(candidate)
            if (
                mode == "siliconflow_tmdb"
                and pick_strong_tmdb_direct_hit([query], db_year, current)[0] is not None
            ):
                break
            if len(found) >= limit:
                break
        return found

    for query_titles in query_groups:
        if mode == "siliconflow_tmdb":
            current = search_queries(
                query_titles,
                lambda q: fetch_tmdb_candidates(
                    q, db_year, is_tv, ctx.tmdb_api_key.get()
                ),
            )
        else:
            current = search_queries(
                query_titles,
                lambda q: fetch_bgm_candidates(q, db_year, ctx.bgm_api_key.get()),
            )
        if current:
            merged.extend(current)
            break

    type_flipped = False
    if not merged and mode == "siliconflow_tmdb":
        flipped_tv = not is_tv
        flipped_year = None if flipped_tv else year
        for query_titles in query_groups:
            current = search_queries(
                query_titles,
                lambda q: fetch_tmdb_candidates(
                    q, flipped_year, flipped_tv, ctx.tmdb_api_key.get()
                ),
            )
            if current:
                merged.extend(current)
                type_flipped = True
                break

    bgm_fallback = False
    if not merged and mode == "siliconflow_tmdb":
        for query_titles in query_groups:
            current = search_queries(
                query_titles,
                lambda q: fetch_bgm_candidates(q, db_year, ctx.bgm_api_key.get()),
            )
            if current:
                merged.extend(current)
                break
        if merged:
            bgm_fallback = True
            source_name = "BGM(回退)"

    if merged:
        title_hit, id_hit, msg_hit, meta_hit = select_best_db_match(
            ctx,
            item,
            used_query,
            db_year,
            is_tv,
            source_name,
            merged,
            recognized_title=query_title,
        )
        if id_hit != "None" and normalize_compare_text(used_query) != normalize_compare_text(query_title):
            msg_hit += " (备选标题)"
        if type_flipped and id_hit != "None":
            msg_hit += " (类型翻转)"
        if bgm_fallback and id_hit != "None":
            meta_hit["_provider"] = "bgm"
            if ctx.tmdb_api_key.get().strip():
                tmdb_candidates = fetch_tmdb_candidates(
                    title_hit or used_query, db_year, is_tv, ctx.tmdb_api_key.get()
                )
                if tmdb_candidates:
                    candidate_meta = tmdb_candidates[0].get("meta") or {}
                    if candidate_meta.get("poster"):
                        meta_hit["poster"] = candidate_meta["poster"]
                    if candidate_meta.get("fanart"):
                        meta_hit["fanart"] = candidate_meta["fanart"]
        return title_hit, id_hit, msg_hit, meta_hit

    return query_title, "None", f"{source_name}无结果", {}


def select_best_db_match(
    ctx,
    item,
    query_title,
    year,
    is_tv,
    source_name,
    candidates,
    recognized_title=None,
):
    if not candidates:
        return query_title, "None", f"{source_name}无结果", {}

    rank_pick_allowed = True
    raw_name = ""
    if isinstance(item, dict):
        raw_name = str(item.get("old_name") or "")
    else:
        raw_name = str(getattr(item, "old_name", "") or "")
    if GROUP_RELEASE_BRACKET_RE.match(raw_name):
        derived_query = derive_title_from_filename(raw_name)
        if (
            derived_query
            and normalize_compare_text(derived_query) != normalize_compare_text(query_title)
        ):
            rank_pick_allowed = False

    if source_name.startswith("TMDb") and not text_mentions_extra_title(
        f"{raw_name} {query_title}"
    ):
        regular_candidates = [
            candidate for candidate in candidates
            if not candidate_looks_like_extra_title(candidate)
        ]
        if regular_candidates:
            candidates = regular_candidates
        elif candidates:
            return query_title, "None", "TMDb候选疑似总集篇/特别篇，需手动确认", {}

    if source_name.startswith("TMDb"):
        source_text = f"{raw_name} {query_title}"
        regular_candidates = [
            candidate
            for candidate in candidates
            if not candidate_looks_like_unrequested_variant(candidate, source_text)
        ]
        if regular_candidates:
            candidates = regular_candidates
        elif candidates:
            return query_title, "None", "TMDb候选疑似外传/衍生剧，需手动确认", {}

    if len(candidates) == 1 and (
        not source_name.startswith("TMDb") or rank_pick_allowed
    ):
        return candidate_to_result(candidates[0], f"{source_name}命中")

    if year:
        year_str = str(year).strip()
        candidates = sorted(
            candidates,
            key=lambda candidate: (
                0 if extract_year_from_release(candidate.get("release") or "") == year_str else 1
            ),
        )

    query_norm = re.sub(r"[\W_]+", "", str(query_title or "").lower())
    requested_year = str(year).strip() if year else ""

    def year_compatible(candidate):
        if not requested_year:
            return True
        candidate_year = extract_year_from_release(candidate.get("release") or "")
        if not candidate_year:
            return True
        return years_within_tolerance(requested_year, candidate_year)

    if query_norm:
        exact = None
        scores = []
        for candidate in candidates:
            candidate_title = re.sub(r"[\W_]+", "", str(candidate.get("title") or "").lower())
            candidate_alt = re.sub(r"[\W_]+", "", str(candidate.get("alt_title") or "").lower())
            candidate_original = re.sub(
                r"[\W_]+",
                "",
                str((candidate.get("meta") or {}).get("original_title") or "").lower(),
            )
            score = max(
                difflib.SequenceMatcher(None, query_norm, candidate_title).ratio()
                if candidate_title else 0.0,
                difflib.SequenceMatcher(None, query_norm, candidate_alt).ratio()
                if candidate_alt else 0.0,
                difflib.SequenceMatcher(None, query_norm, candidate_original).ratio()
                if candidate_original else 0.0,
            )
            scores.append((score, candidate))
            if (
                candidate_title == query_norm
                or candidate_alt == query_norm
                or candidate_original == query_norm
            ) and year_compatible(candidate):
                exact = candidate
                break
        if exact is None and scores:
            scores.sort(key=lambda row: row[0], reverse=True)
            top_score, top_candidate = scores[0]
            second_score = scores[1][0] if len(scores) > 1 else 0.0
            if top_score >= 0.90 and (top_score - second_score) >= 0.20 and year_compatible(top_candidate):
                exact = top_candidate
        if exact is not None:
            return candidate_to_result(exact, f"标题匹配/{source_name}命中")

    if source_name.startswith("TMDb") and rank_pick_allowed:
        direct_hit, matched_query = pick_strong_tmdb_direct_hit(
            [query_title, recognized_title], year, candidates
        )
        if direct_hit is not None:
            hit_msg = f"TMDb直搜首位/{source_name}命中"
            if matched_query and normalize_compare_text(matched_query) != normalize_compare_text(query_title):
                hit_msg += " (别名直搜)"
            return candidate_to_result(direct_hit, hit_msg)

    score_pick, score_reason = ctx._auto_pick_candidate_by_score(
        query_title, year, source_name, candidates
    )
    if score_pick is not None:
        return candidate_to_result(score_pick, f"自动评分/{source_name}命中 ({score_reason})")

    prefer_ollama = bool(ctx.prefer_ollama.get())
    online_ready = ctx._can_use_online_model_for_pick()
    ollama_ready = ctx._can_use_ollama_for_pick()
    ranked, _, embedding_msg = ctx._rerank_candidates_with_embedding(
        item, query_title, year, is_tv, source_name, candidates
    )

    def candidate_result_from_model(label, chosen, reason):
        hit_msg = f"{label}/{source_name}命中"
        if embedding_msg:
            hit_msg += f" ({embedding_msg})"
        if reason:
            hit_msg += f" ({reason})"
        return candidate_to_result(chosen, hit_msg)

    ai_attempted = False

    if prefer_ollama and ollama_ready:
        ai_attempted = True
        chosen, reason = ctx._pick_candidate_with_ollama(
            item, query_title, year, is_tv, source_name, ranked
        )
        if chosen:
            return candidate_result_from_model("Ollama判定", chosen, reason)

    if online_ready:
        ai_attempted = True
        chosen, reason = ctx._pick_candidate_with_online_model(
            item, query_title, year, is_tv, source_name, ranked
        )
        if chosen:
            return candidate_result_from_model("在线模型判定", chosen, reason)

    if (not prefer_ollama) and ollama_ready:
        ai_attempted = True
        chosen, reason = ctx._pick_candidate_with_ollama(
            item, query_title, year, is_tv, source_name, ranked
        )
        if chosen:
            return candidate_result_from_model("Ollama判定", chosen, reason)

    pending_reason = "候选存在歧义，需手动确认"
    if ai_attempted:
        pending_reason = "候选存在歧义，AI未能稳定判定"
    elif not (online_ready or ollama_ready):
        pending_reason = "候选存在歧义，未启用AI自动判定"
    if embedding_msg:
        pending_reason += f" ({embedding_msg})"

    return query_title, "None", pending_reason, {}
