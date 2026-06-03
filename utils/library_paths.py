import os
import re

from utils.title_parsing import (
    GENERIC_SEASON_TITLE_RE,
    clean_search_title,
    normalize_search_query_title,
    title_variant_markers,
)
from utils.value_utils import normalize_compare_text, safe_int


LIBRARY_SEASON_DIR_RE = re.compile(
    r"(?i)^(?:Season\s*0*(\d{1,2})|第\s*0*(\d{1,2})\s*季)$"
)
_FOLDER_TMDBID_RE = re.compile(r"(?i)tmdb(?:id)?[-=:_](\d{1,8})")
_FOLDER_BGMID_RE = re.compile(r"(?i)bgm(?:id)?[-=:_](\d{1,8})")
_FOLDER_DB_TAG_CLEAN_RE = re.compile(
    r"(?i)[\[\(\{]?\s*(?:tmdb|tmdbid|bgm|bgmid)[-=:_]\d{1,8}\s*[\]\)\}]?"
)
_FILENAME_TMDBID_RE = re.compile(r"(?i)[\[{]\s*tmdb(?:id)?[-=](\d{1,8})\s*[\]}]")
_FILENAME_BGMID_RE = re.compile(r"(?i)[\[{]\s*bgm(?:id)?[-=](\d{1,8})\s*[\]}]")
_FILENAME_DOUBANID_RE = re.compile(r"(?i)[\[{]\s*douban(?:id)?[-=](\d{1,8})\s*[\]}]")


def _latin_title_tokens(text):
    stopwords = {"the", "and", "for", "with", "from", "that", "this", "into"}
    return {
        tok.lower()
        for tok in re.findall(r"[A-Za-z]{3,}", str(text or ""))
        if tok and tok.lower() not in stopwords
    }


def _folder_title_conflicts_with_hints(folder_title, title_hints):
    folder_text = clean_search_title(_FOLDER_DB_TAG_CLEAN_RE.sub(" ", folder_title))
    if not folder_text or GENERIC_SEASON_TITLE_RE.match(folder_text):
        return False

    folder_markers = title_variant_markers(folder_text)
    hint_marker_union = set()
    hint_norms = []
    hint_latin_sets = []
    hint_has_meaningful_title = False
    for raw in title_hints or []:
        for candidate in (
            clean_search_title(raw),
            normalize_search_query_title(raw),
        ):
            key = normalize_compare_text(candidate)
            if not key:
                continue
            hint_has_meaningful_title = True
            hint_norms.append(key)
            hint_latin_sets.append(_latin_title_tokens(candidate))
            hint_marker_union.update(title_variant_markers(candidate))

    if not hint_has_meaningful_title:
        return False

    if folder_markers and not folder_markers.issubset(hint_marker_union):
        return True

    folder_norms = []
    for candidate in (
        folder_text,
        normalize_search_query_title(folder_text),
    ):
        key = normalize_compare_text(candidate)
        if key and key not in folder_norms:
            folder_norms.append(key)

    for folder_norm in folder_norms:
        for hint_norm in hint_norms:
            if folder_norm == hint_norm:
                return False
            if len(folder_norm) >= 6 and len(hint_norm) >= 6:
                shorter, longer = sorted((folder_norm, hint_norm), key=len)
                if longer.startswith(shorter) or shorter in longer:
                    return False

    folder_latin = _latin_title_tokens(folder_text)
    if not folder_latin:
        return False

    for hint_latin in hint_latin_sets:
        if not hint_latin:
            continue
        shared = folder_latin & hint_latin
        if len(shared) >= min(len(folder_latin), len(hint_latin), 2):
            return False

    if len(folder_latin) >= 2 and any(len(tokens) >= 2 for tokens in hint_latin_sets):
        return True
    return False


def extract_db_id_from_path(path, mode, title_hints=None):
    """Extract tmdbid or bgmid from directory components and filename."""
    path_str = str(path or "")
    filename = os.path.basename(path_str)
    fn_pat = _FILENAME_TMDBID_RE if mode == "siliconflow_tmdb" else _FILENAME_BGMID_RE
    fn_match = fn_pat.search(filename)
    if fn_match:
        return fn_match.group(1)

    dir_part = os.path.dirname(path_str)
    pat = _FOLDER_TMDBID_RE if mode == "siliconflow_tmdb" else _FOLDER_BGMID_RE
    parts = [part for part in re.split(r"[\\/]+", dir_part) if part]
    for part in reversed(parts):
        match = pat.search(part)
        if not match:
            continue
        if title_hints and _folder_title_conflicts_with_hints(part, title_hints):
            continue
        return match.group(1)
    return None


def build_existing_library_target(file_path, new_name, metadata):
    """Reuse the current library folder when the file is already organized."""
    path_str = str(file_path or "").strip()
    target_name = str(new_name or "").strip()
    meta = metadata or {}
    media_id = str(meta.get("id") or "").strip()
    provider = str(meta.get("provider") or "").strip().lower()
    media_type = str(meta.get("type") or "").strip().lower()

    if not path_str or not target_name or not media_id or media_id == "None":
        return ""
    if provider not in {"tmdb", "bgm"}:
        return ""

    mode = "siliconflow_tmdb" if provider == "tmdb" else "siliconflow_bgm"
    title_hints = [meta.get("title"), meta.get("original_title")]
    current_dir = os.path.dirname(os.path.normpath(path_str))
    if not current_dir:
        return ""

    if media_type == "episode":
        season_match = LIBRARY_SEASON_DIR_RE.match(os.path.basename(current_dir))
        if not season_match:
            return ""
        try:
            current_season = int(season_match.group(1) or season_match.group(2))
        except (TypeError, ValueError):
            return ""

        expected_season = safe_int(meta.get("s"), current_season)
        if current_season != expected_season:
            return ""

        series_root = os.path.dirname(current_dir)
        if not series_root:
            return ""
        existing_id = extract_db_id_from_path(
            os.path.join(series_root, os.path.basename(path_str)),
            mode,
            title_hints=title_hints,
        )
        if str(existing_id or "") != media_id:
            return ""
        return os.path.join(current_dir, target_name)

    if media_type == "movie":
        existing_id = extract_db_id_from_path(path_str, mode, title_hints=title_hints)
        if str(existing_id or "") != media_id:
            return ""
        return os.path.join(current_dir, target_name)

    return ""
