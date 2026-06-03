import os
import re
import time

from ai.ollama_ai import fetch_siliconflow_info, is_ai_rate_limited_error
from core.services.season_rules import extract_season_from_dir
from utils.title_parsing import derive_title_from_filename, extract_episode_number
from utils.value_utils import normalize_compare_text, safe_int


SPECIAL_TAG_RE = re.compile(r"(?i)(?<![A-Z0-9])(?:PROLOGUE|OVA|OAD|SP|SPECIAL|NC\.VER|EXTRA)(?![A-Z0-9])")
SPECIAL_EPISODE_RE = re.compile(r"(?i)(?<![A-Z0-9])(?:SP|OVA|OAD|SPECIAL|EXTRA)(?![A-Z0-9])\s*(?:BD)?\s*0*(\d+)")
PROLOGUE_RE = re.compile(r"(?i)(?<![A-Z0-9])PROLOGUE(?![A-Z0-9])")
GROUP_RELEASE_RE = re.compile(r"^(?:\[[^\]]+\]\s*){2,}")
GENERIC_TITLE_RE = re.compile(
    r"(?i)^(?:unknown|none|null|untitled|na|nan|未知|season\s*\d{1,2}|s\s*\d{1,2}|第\s*\d{1,2}\s*季)$"
)
GENERIC_SEASON_DIR_RE = re.compile(r"(?i)^(?:season\s*\d{1,2}|s\s*\d{1,2}|第\s*\d{1,2}\s*季)$")
STANDARD_EPISODE_RE = re.compile(r"(?i)\bS\d{1,2}E\d{1,3}\b")
ZERO_EPISODE_SPECIAL_RE = re.compile(r"(?i)\bS\s*0*(\d{1,2})\s*E\s*0*0\b")
ALT_ZERO_EPISODE_SPECIAL_RE = re.compile(r"(?i)\b(\d{1,2})x0*0\b")
DECIMAL_EPISODE_RE = re.compile(
    r"""(?ix)
    (?:
        s\d{1,2}\s*e\d{1,4}\.\d(?!\d)
        | (?:ep?)\s*0*\d{1,4}\.\d(?!\d)
        | 第\s*0*\d{1,4}\.\d(?!\d)\s*[集话話]
        | [\[\(（]\s*0*\d{1,4}\.\d(?!\d)\s*[\]\)）]
        | (?<![.\d])-\s*0*\d{1,2}\.\d(?!\d)(?=[\s\[\(（]|$)
    )"""
)
AI_RATE_LIMIT_COOLDOWN_SECONDS = 60.0


def is_meaningful_title(title):
    raw = str(title or "").strip()
    if not raw:
        return False
    if GENERIC_TITLE_RE.match(raw):
        return False
    return bool(normalize_compare_text(raw))


def season_value_or_default(value, default=1):
    if value in (None, ""):
        return default
    return safe_int(value, default)


def extract_zero_episode_special_slot(pure_name):
    text = str(pure_name or "")
    for pattern in (ZERO_EPISODE_SPECIAL_RE, ALT_ZERO_EPISODE_SPECIAL_RE):
        match = pattern.search(text)
        if not match:
            continue
        season_num = safe_int(match.group(1), -1)
        if 1 <= season_num <= 99:
            return season_num
    return None


def is_decimal_recap_episode(pure_name):
    return bool(DECIMAL_EPISODE_RE.search(str(pure_name or "")))


def fetch_ai_parse(gui, pure_for_parse):
    def remaining_remote_ai_cooldown():
        until = float(getattr(gui, "remote_ai_cooldown_until", 0.0) or 0.0)
        return max(0.0, until - time.monotonic())

    def wait_remote_ai_cooldown():
        while True:
            remaining = remaining_remote_ai_cooldown()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 1.0))

    def set_remote_ai_cooldown():
        until = time.monotonic() + AI_RATE_LIMIT_COOLDOWN_SECONDS
        with gui.cache_lock:
            current = float(getattr(gui, "remote_ai_cooldown_until", 0.0) or 0.0)
            gui.remote_ai_cooldown_until = max(current, until)

    def fetch_remote():
        wait_remote_ai_cooldown()
        result = fetch_siliconflow_info(
            pure_for_parse,
            gui.sf_api_key.get(),
            gui.sf_api_url.get(),
            gui.sf_model.get(),
            gui._get_ai_temperature(),
            gui._get_ai_top_p(),
        )
        if not result[0] and is_ai_rate_limited_error(result[1]):
            set_remote_ai_cooldown()
        return result

    if gui.prefer_ollama.get():
        if gui.ollama_url.get().strip() and gui.ollama_model.get().strip():
            return gui._parse_with_ollama(pure_for_parse)
        if gui.sf_api_key.get().strip():
            return fetch_remote()
        return None, ""

    if gui.sf_api_key.get().strip():
        return fetch_remote()
    return None, ""


def derive_guessit_fields(gui, pure, dir_p, guess_data, extracted_ep):
    title = guess_data.get("title") or derive_title_from_filename(pure) or "未知"
    dir_season = extract_season_from_dir(dir_p)
    season = gui._pick_season(pure, guess_data, dir_season if dir_season is not None else 1)
    year = guess_data.get("year")
    if year and season > 1:
        year = None

    if not year:
        year_dir = os.path.dirname(dir_p) if dir_season is not None else dir_p
        for _ in range(4):
            folder_name = os.path.basename(year_dir)
            if season > 1 and gui._extract_explicit_season(folder_name) is not None:
                parent_dir = os.path.dirname(year_dir)
                if not parent_dir or parent_dir == year_dir:
                    break
                year_dir = parent_dir
                continue
            year_match = re.search(r"\b((?:19|20)\d{2})\b", folder_name)
            if year_match:
                year = int(year_match.group(1))
                break
            parent_dir = os.path.dirname(year_dir)
            if not parent_dir or parent_dir == year_dir:
                break
            year_dir = parent_dir
    episode = extracted_ep if extracted_ep is not None else 1
    return title, year, season, episode


def guessit_needs_assist(pure, dir_p, guess_data, title, extracted_ep):
    title_norm = normalize_compare_text(title)
    if not is_meaningful_title(title):
        return True
    if len(title_norm) <= 2:
        return True
    if extracted_ep is None:
        return True
    if GROUP_RELEASE_RE.search(str(pure or "")):
        return True

    guess_title = str(guess_data.get("title") or "").strip()
    derived_title = derive_title_from_filename(pure)
    if (
        guess_title
        and is_meaningful_title(derived_title)
        and normalize_compare_text(guess_title) != normalize_compare_text(derived_title)
    ):
        return True

    looks_like_clean_standard_episode = (
        extracted_ep is not None
        and is_meaningful_title(title)
        and str((guess_data or {}).get("type") or "").strip().lower() == "episode"
        and (STANDARD_EPISODE_RE.search(str(pure or "")) or safe_int((guess_data or {}).get("season"), 0) > 0)
    )

    dir_name = os.path.basename(dir_p or "").strip()
    if GENERIC_SEASON_DIR_RE.match(dir_name) and not looks_like_clean_standard_episode:
        parent_title = os.path.basename(os.path.dirname(dir_p or "")).strip()
        if is_meaningful_title(parent_title):
            if normalize_compare_text(parent_title) != title_norm:
                return True

    return False


def merge_assist_parse(
    gui,
    pure,
    dir_p,
    guess_data,
    guess_title,
    guess_year,
    guess_season,
    guess_episode,
    extracted_ep,
    ai_data,
):
    title = guess_title
    year = guess_year
    season = guess_season
    episode = guess_episode
    used_fields = set()

    ai_title = str((ai_data or {}).get("title") or "").strip()
    ai_year = (ai_data or {}).get("year")
    ai_season = safe_int((ai_data or {}).get("season"), 1)
    ai_episode = extract_episode_number(pure, None, ai_data) or safe_int((ai_data or {}).get("episode"), 1)

    if is_meaningful_title(ai_title):
        if not is_meaningful_title(title):
            title = ai_title
            used_fields.add("title")
        elif normalize_compare_text(ai_title) != normalize_compare_text(title):
            title = ai_title
            used_fields.add("title")

    if ai_year and (not year or "title" in used_fields):
        year = ai_year
        used_fields.add("year")

    explicit_season = gui._extract_explicit_season(pure)
    dir_season = extract_season_from_dir(dir_p)
    if explicit_season is None and dir_season is None and ai_season >= 1:
        if ai_season != safe_int(season, 1):
            season = gui._pick_season(pure, guess_data, ai_season)
            used_fields.add("season")

    if extracted_ep is None and ai_episode:
        if ai_episode != safe_int(episode, 1):
            episode = ai_episode
            used_fields.add("episode")

    guessit_reliable = is_meaningful_title(guess_title) and extracted_ep is not None
    if used_fields:
        parse_source = "hybrid" if guessit_reliable else "ai"
    else:
        parse_source = "guessit"

    return title, year, season, episode, parse_source
