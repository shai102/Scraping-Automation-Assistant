import os
import re

from utils.media_defaults import DEFAULT_SUB_AUDIO_EXTS, DEFAULT_VIDEO_EXTS
from utils.title_cleanup import (
    GROUP_RELEASE_BRACKET_RE,
    LEADING_RELEASE_GROUP_RE,
    clean_search_title,
    derive_title_from_filename,
    extract_bracket_title_from_filename,
    extract_title_after_leading_release_group,
    is_meaningful_query_title,
    normalize_search_query_title,
    split_mixed_title,
)
from utils.value_utils import normalize_compare_text


def unique_keep_order(values):
    seen = set()
    out = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = normalize_compare_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def build_query_titles(item, query_title, ai_data, g):
    if isinstance(item, dict):
        raw_name = item.get("old_name", "")
        item_dir = item.get("dir", "") or ""
    else:
        raw_name = getattr(item, "old_name", "") or ""
        item_dir = getattr(item, "dir", "") or ""

    known_exts = set(
        ext.strip().lower()
        for ext in (DEFAULT_VIDEO_EXTS + "," + DEFAULT_SUB_AUDIO_EXTS).split(",")
        if ext.strip()
    )
    pure = raw_name
    for _ in range(3):
        base, ext = os.path.splitext(pure)
        if ext.lower() in known_exts:
            pure = base
        else:
            break

    dir_title = os.path.basename(item_dir)
    guess_title = clean_search_title((g.get("title") if g else None) or "")
    ai_title = clean_search_title(
        (ai_data or {}).get("title") if isinstance(ai_data, dict) else ""
    )

    show_dir_title = ""
    if item_dir and clean_search_title(dir_title):
        from utils.title_cleanup import GENERIC_SEASON_TITLE_RE

        if GENERIC_SEASON_TITLE_RE.match(dir_title.strip()):
            parent_dir = os.path.dirname(item_dir)
            if parent_dir:
                show_dir_title = clean_search_title(os.path.basename(parent_dir))

    candidates = [
        query_title,
        (ai_data or {}).get("title") if isinstance(ai_data, dict) else None,
        guess_title,
        extract_title_after_leading_release_group(pure),
        extract_bracket_title_from_filename(pure),
        derive_title_from_filename(pure),
        show_dir_title,
        clean_search_title(dir_title),
        clean_search_title(pure),
    ]

    for candidate in list(candidates):
        if candidate:
            candidates.extend(split_mixed_title(candidate))

    expanded_candidates = []
    for candidate in candidates:
        raw_clean = clean_search_title(candidate)
        normalized = normalize_search_query_title(candidate)
        raw_norm = normalize_compare_text(raw_clean)
        normalized_norm = normalize_compare_text(normalized)
        preserve_raw_alias = (
            raw_clean
            and normalized
            and raw_norm
            and normalized_norm
            and raw_norm != normalized_norm
            and bool(re.search(r"[._/]", raw_clean))
            and len(raw_norm) <= len(normalized_norm) + 4
        )
        if raw_clean and preserve_raw_alias:
            expanded_candidates.append(raw_clean)
        if normalized:
            expanded_candidates.append(normalized)

    ordered = unique_keep_order(expanded_candidates)
    protected_norms = {
        normalize_compare_text(text)
        for text in (clean_search_title(query_title), guess_title, ai_title)
        if is_meaningful_query_title(text)
    }
    strong_norms = [
        normalize_compare_text(text)
        for text in ordered
        if len(str(text or "").split()) >= 2 or len(normalize_compare_text(text)) >= 8
    ]
    filtered = []
    for text in ordered:
        if not is_meaningful_query_title(text):
            continue
        norm = normalize_compare_text(text)
        if (
            len(str(text or "").split()) == 1
            and len(norm) <= 2
            and re.fullmatch(r"[A-Za-z']+", str(text or ""))
            and norm not in protected_norms
        ):
            continue
        if (
            len(str(text or "").split()) == 1
            and len(norm) < 8
            and norm not in protected_norms
            and any(norm != strong and norm in strong for strong in strong_norms)
        ):
            continue
        filtered.append(text)
    return filtered


def build_db_query_plan(item, query_title, ai_data, g):
    if isinstance(item, dict):
        raw_name = item.get("old_name", "")
    else:
        raw_name = getattr(item, "old_name", "") or ""

    known_exts = set(
        ext.strip().lower()
        for ext in (DEFAULT_VIDEO_EXTS + "," + DEFAULT_SUB_AUDIO_EXTS).split(",")
        if ext.strip()
    )
    pure = raw_name
    for _ in range(3):
        base, ext = os.path.splitext(pure)
        if ext.lower() in known_exts:
            pure = base
        else:
            break

    query_titles = build_query_titles(item, query_title, ai_data, g)
    if not query_titles:
        return []

    ai_title = clean_search_title(
        (ai_data or {}).get("title") if isinstance(ai_data, dict) else ""
    )
    guess_title = clean_search_title((g.get("title") if g else None) or "")
    derived_title = derive_title_from_filename(pure)

    if ai_title and (
        not is_meaningful_query_title(guess_title)
        or normalize_compare_text(guess_title) != normalize_compare_text(ai_title)
    ):
        if is_meaningful_query_title(guess_title) and (
            normalize_compare_text(guess_title) != normalize_compare_text(ai_title)
        ):
            return [[ai_title], [guess_title]]
        return [[ai_title]]

    if (
        derived_title
        and LEADING_RELEASE_GROUP_RE.match(pure)
        and normalize_compare_text(clean_search_title(pure))
        != normalize_compare_text(derived_title)
    ):
        return [[derived_title]]

    if (
        derived_title
        and GROUP_RELEASE_BRACKET_RE.match(pure)
        and (
            not is_meaningful_query_title(guess_title)
            or normalize_compare_text(guess_title) != normalize_compare_text(derived_title)
        )
    ):
        return [[derived_title]]
    return [query_titles]
