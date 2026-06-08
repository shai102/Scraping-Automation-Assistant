import json
import re


_GENERIC_EP_TITLE_RE = re.compile(r"^第\s*(?:\d+|[零〇一二两三四五六七八九十百千万]+)\s*集$")


def _split_rules(rules) -> list[str]:
    if not rules:
        return []
    if isinstance(rules, (list, tuple, set)):
        raw_items = rules
    else:
        raw_items = re.split(r"[\n,，;；]+", str(rules))
    return [str(item or "").strip() for item in raw_items if str(item or "").strip()]


def _norm_text(value) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip().lower()


def _metadata_matches_rules(
    metadata: dict,
    rules=None,
    *,
    title_hint: str = "",
    matched_id: str = "",
    provider_hint: str = "",
) -> bool:
    parsed_rules = _split_rules(rules)
    if not parsed_rules:
        return False

    provider = _norm_text(metadata.get("provider") or metadata.get("_provider") or provider_hint)
    ids = {
        _norm_text(metadata.get("id")),
        _norm_text(metadata.get("tmdb_id")),
        _norm_text(metadata.get("bgm_id")),
        _norm_text(matched_id),
    }
    ids.discard("")
    provider_ids = {f"{provider}:{item}" for item in ids if provider}
    if _norm_text(metadata.get("tmdb_id")):
        provider_ids.add(f"tmdb:{_norm_text(metadata.get('tmdb_id'))}")
    if _norm_text(metadata.get("bgm_id")):
        provider_ids.add(f"bgm:{_norm_text(metadata.get('bgm_id'))}")
    title_values = {
        _norm_text(metadata.get("title")),
        _norm_text(metadata.get("original_title")),
        _norm_text(metadata.get("alt_title")),
        _norm_text(title_hint),
    }
    title_values.discard("")

    for rule in parsed_rules:
        normalized = _norm_text(rule)
        if not normalized:
            continue
        if normalized in ids or normalized in provider_ids or normalized in title_values:
            return True
    return False


def _episode_title_is_ignored(
    metadata: dict,
    rules=None,
    *,
    title_hint: str = "",
    matched_id: str = "",
    provider_hint: str = "",
) -> bool:
    return _metadata_matches_rules(
        metadata,
        rules,
        title_hint=title_hint,
        matched_id=matched_id,
        provider_hint=provider_hint,
    )


def metadata_missing_fields(
    metadata_json_str: str,
    *,
    ignore_episode_title_rules=None,
    skip_rules=None,
    title_hint: str = "",
    matched_id: str = "",
    provider_hint: str = "",
) -> list[str]:
    """Return human-readable missing metadata field names for a stored record."""
    missing = []
    if not metadata_json_str:
        return missing
    try:
        metadata = json.loads(metadata_json_str)
    except Exception:
        return missing

    record_matched_id = str(matched_id or "").strip()
    matched_id = str(metadata.get("id") or record_matched_id or "None")
    if matched_id == "None" or not matched_id:
        return missing
    if _metadata_matches_rules(
        metadata,
        skip_rules,
        title_hint=title_hint,
        matched_id=record_matched_id or matched_id,
        provider_hint=provider_hint,
    ):
        return missing

    media_type = str(metadata.get("type") or "episode").strip().lower()
    is_tv = media_type == "episode"

    if is_tv:
        ep_title = str(metadata.get("ep_title") or "").strip()
        if (
            not _episode_title_is_ignored(
                metadata,
                ignore_episode_title_rules,
                title_hint=title_hint,
                matched_id=matched_id,
                provider_hint=provider_hint,
            )
            and (not ep_title or _GENERIC_EP_TITLE_RE.match(ep_title))
        ):
            missing.append("集标题")
        if not str(metadata.get("ep_plot") or "").strip():
            missing.append("集简介")
        if not str(metadata.get("still") or "").strip():
            missing.append("剧照")
        if not str(metadata.get("overview") or "").strip():
            missing.append("作品简介")
        if not metadata.get("actors"):
            missing.append("演员")
        if not metadata.get("genres"):
            missing.append("类型")
        try:
            if float(metadata.get("rating") or 0) == 0:
                missing.append("评分")
        except (TypeError, ValueError):
            missing.append("评分")
    else:
        if not str(metadata.get("overview") or "").strip():
            missing.append("作品简介")
        if not metadata.get("actors"):
            missing.append("演员")
        if not metadata.get("genres"):
            missing.append("类型")
        if not str(metadata.get("poster") or "").strip():
            missing.append("海报")
        if not str(metadata.get("fanart") or "").strip():
            missing.append("背景图")
        try:
            if float(metadata.get("rating") or 0) == 0:
                missing.append("评分")
        except (TypeError, ValueError):
            missing.append("评分")

    return missing


def metadata_is_incomplete(metadata_json_str: str, **kwargs) -> bool:
    """Return True when critical metadata fields are still missing."""
    return bool(metadata_missing_fields(metadata_json_str, **kwargs))
