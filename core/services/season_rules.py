import difflib
import os
import re

from utils.media_patterns import VERSION_TAG_RE
from utils.title_parsing import clean_search_title, derive_title_from_filename
from utils.value_utils import normalize_compare_text, safe_int, safe_str


def extract_explicit_season(pure_name):
    text = str(pure_name or "")
    s_prefix_patterns = [
        r"(?i)\bS\s*0*(\d{1,2})\s*E\s*0*\d{1,4}\b",
        r"(?i)\bS\s*0*(\d{1,2})\b",
    ]
    for pattern in s_prefix_patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        season_num = safe_int(match.group(1), -1)
        if 0 <= season_num <= 99:
            return season_num

    other_patterns = [
        r"(?i)\bSeason\s*0*(\d{1,2})\b",
        r"(?i)\b(\d{1,2})(?:st|nd|rd|th)\s*Season\b",
        r"第\s*0*(\d{1,2})\s*季",
    ]
    for pattern in other_patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        season_num = safe_int(match.group(1), 0)
        if 1 <= season_num <= 99:
            return season_num
    return None


def extract_season_from_dir(dir_path):
    folder_name = os.path.basename(str(dir_path or ""))
    patterns = [
        r"(?i)\bSeason\s*0*(\d{1,2})\b",
        r"第\s*0*(\d{1,2})\s*季",
    ]
    for pattern in patterns:
        match = re.search(pattern, folder_name)
        if match:
            season_num = safe_int(match.group(1), -1)
            if 0 <= season_num <= 99:
                return season_num
    return None


def pick_season(pure_name, guess_data=None, fallback=1):
    explicit = extract_explicit_season(pure_name)
    if explicit is not None:
        return explicit

    guessed = safe_int((guess_data or {}).get("season"), 0)
    if 0 < guessed <= 99:
        return guessed

    fallback_num = safe_int(fallback, 1)
    if 1 <= fallback_num <= 99:
        return fallback_num
    return 1


def can_reuse_dir_ai(cached_ai, pure_name, guess_data=None):
    if not isinstance(cached_ai, dict):
        return False

    cached_titles = [clean_search_title(cached_ai.get("title") or "")]
    for alias in cached_ai.get("title_aliases") or []:
        cached_titles.append(clean_search_title(alias or ""))

    cached_keys = [normalize_compare_text(title) for title in cached_titles]
    cached_keys = [key for key in cached_keys if key]
    if not cached_keys:
        return False

    cached_year = safe_str(cached_ai.get("year"))
    guess_year = safe_str((guess_data or {}).get("year"))
    if cached_year and guess_year and cached_year != guess_year:
        return False

    title_candidates = [
        clean_search_title((guess_data or {}).get("title") or ""),
        derive_title_from_filename(pure_name),
    ]
    for candidate in title_candidates:
        cand_key = normalize_compare_text(candidate)
        if not cand_key:
            continue
        for cached_key in cached_keys:
            if cand_key == cached_key:
                return True
            if len(cand_key) >= 4 and len(cached_key) >= 4:
                ratio = difflib.SequenceMatcher(None, cand_key, cached_key).ratio()
                if ratio >= 0.85:
                    return True
                shorter, longer = (
                    (cand_key, cached_key)
                    if len(cand_key) <= len(cached_key)
                    else (cached_key, cand_key)
                )
                if longer.startswith(shorter) and len(shorter) >= 4:
                    return True
    return False


def get_version_tag(path):
    match = VERSION_TAG_RE.search(os.path.basename(path))
    return f" {match.group(0)}" if match else ""
