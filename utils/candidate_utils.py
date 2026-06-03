from utils.value_utils import extract_year_from_release, normalize_compare_text


def format_candidate_label(candidate):
    title = candidate.get("title") or "未知"
    alt_title = candidate.get("alt_title") or ""
    if alt_title and normalize_compare_text(alt_title) == normalize_compare_text(title):
        alt_title = ""
    year = extract_year_from_release(candidate.get("release")) or "-"
    rating = candidate.get("rating")
    try:
        rating_text = (
            f"{float(rating):.1f}" if rating not in (None, "", 0, "0") else "-"
        )
    except Exception:
        rating_text = "-"
    parts = [title]
    if alt_title:
        parts.append(f"原名:{alt_title}")
    parts.append(f"年份:{year}")
    parts.append(f"评分:{rating_text}")
    parts.append(f"ID:{candidate.get('id', '-')}")
    source = candidate.get("msg")
    if source:
        parts.append(str(source))
    return " | ".join(parts)


def candidate_to_result(candidate, hit_msg):
    if not candidate:
        return "", "None", hit_msg, {}
    return (
        candidate.get("title") or "",
        str(candidate.get("id", "None")),
        hit_msg,
        candidate.get("meta") or {},
    )
