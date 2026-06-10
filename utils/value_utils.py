import re


def safe_filename(text):
    """Normalize illegal path chars and trim dangerous suffixes."""
    if not text:
        return ""
    illegal_chars = r'<>:"/\\|?*' + chr(0)
    for char in illegal_chars:
        text = text.replace(char, "_")
    text = text.strip().strip(".")
    if len(text) > 200:
        text = text[:200]
    return text


def normalize_compare_text(text):
    if not text:
        return ""
    text = str(text).lower()
    return re.sub(r"[\W_]+", "", text, flags=re.UNICODE)


def extract_year_from_release(release):
    if not release:
        return ""
    match = re.search(r"(\d{4})", str(release))
    return match.group(1) if match else ""


def years_match_exact(year_a, year_b):
    """True only when both years are present and identical."""
    a = extract_year_from_release(year_a)
    b = extract_year_from_release(year_b)
    return bool(a) and bool(b) and a == b


def years_within_tolerance(year_a, year_b, tolerance=1):
    """True when years are missing on either side, or differ by <= tolerance.

    Regional release dates (e.g. JP air date vs TMDb regional year) frequently
    differ by one year, especially for cross-year anime seasons. Treat such
    near-misses as compatible instead of rejecting the candidate outright.
    """
    a = extract_year_from_release(year_a)
    b = extract_year_from_release(year_b)
    if not a or not b:
        return True
    return abs(int(a) - int(b)) <= tolerance


def normalize_parse_source(parse_source):
    raw = str(parse_source or "").strip().lower()
    if raw == "guessit":
        return "guessit"
    if raw in {"ai", "hybrid"}:
        return "ai"
    return raw


def safe_str(val):
    if val is None:
        return ""
    if isinstance(val, list):
        if val:
            return str(val[0])
        return ""
    return str(val)


def safe_int(value, default=1):
    try:
        if isinstance(value, list):
            value = value[0] if value else default
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            match = re.search(r"[-+]?\d+", value)
            return int(match.group()) if match else default
        return default
    except (ValueError, TypeError):
        return default
