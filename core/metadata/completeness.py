import json
import re


_GENERIC_EP_TITLE_RE = re.compile(r"^第\s*\d+\s*集$")


def metadata_missing_fields(metadata_json_str: str) -> list[str]:
    """Return human-readable missing metadata field names for a stored record."""
    missing = []
    if not metadata_json_str:
        return missing
    try:
        metadata = json.loads(metadata_json_str)
    except Exception:
        return missing

    matched_id = str(metadata.get("id") or "None")
    if matched_id == "None" or not matched_id:
        return missing

    media_type = str(metadata.get("type") or "episode").strip().lower()
    is_tv = media_type == "episode"

    if is_tv:
        ep_title = str(metadata.get("ep_title") or "").strip()
        if not ep_title or _GENERIC_EP_TITLE_RE.match(ep_title):
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


def metadata_is_incomplete(metadata_json_str: str) -> bool:
    """Return True when critical metadata fields are still missing."""
    return bool(metadata_missing_fields(metadata_json_str))
