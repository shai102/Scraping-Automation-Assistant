import re

from .annotation_common import field_note, split_csv_values


METADATA_SCAN_START_RE = re.compile(r"^元数据巡检:\s*发现 (?P<count>\d+) 条不完整记录，开始刷新$")
METADATA_SCAN_ITEM_RE = re.compile(
    r"^元数据巡检项:\s*record_id=(?P<record_id>\d+)\s+\|\s+title=(?P<title>.*?)\s+\|\s+"
    r"target_path=(?P<target_path>.*?)\s+\|\s+missing_fields=(?P<missing_fields>.+)$"
)
METADATA_REFRESH_RE = re.compile(
    r"^元数据刷新:\s*record_id=(?P<record_id>\d+)\s+\|\s+title=(?P<title>.*?)\s+\|\s+"
    r"id=(?P<id>.*?)\s+\|\s+provider=(?P<provider>.*?)\s+\|\s+"
    r"target_path=(?P<target_path>.*?)\s+\|\s+updated_fields=(?P<updated_fields>.+)$"
)
METADATA_REFRESH_FAIL_RE = re.compile(
    r"^元数据刷新失败:\s*record_id=(?P<record_id>\d+)\s+\|\s+title=(?P<title>.*?)\s+\|\s+"
    r"target_path=(?P<target_path>.*?)\s+\|\s+reason=(?P<reason>.+)$"
)
METADATA_SCAN_DONE_RE = re.compile(r"^元数据巡检完成:\s*刷新了 (?P<refreshed>\d+)/(?P<total>\d+) 条记录$")


def annotate_metadata_scan_start(message: str) -> dict | None:
    match = METADATA_SCAN_START_RE.match(message)
    if not match:
        return None
    count = match.group("count").strip()
    return {
        "kind": "metadata_scan_start",
        "parsed": {"count": count},
        "annotation": {
            "title": "元数据巡检开始",
            "summary": f"后台巡检发现 {count} 条元数据不完整记录，准备逐条刷新。",
            "items": [
                {"field": "count", "label": "count", "value": count, "note": field_note("count", count)},
            ],
        },
    }


def annotate_metadata_scan_item(message: str) -> dict | None:
    match = METADATA_SCAN_ITEM_RE.match(message)
    if not match:
        return None
    parsed = {
        "record_id": match.group("record_id").strip(),
        "title": match.group("title").strip(),
        "target_path": match.group("target_path").strip(),
        "missing_fields": match.group("missing_fields").strip(),
    }
    return {
        "kind": "metadata_scan_item",
        "parsed": parsed,
        "annotation": {
            "title": "元数据不完整记录",
            "summary": "这条记录会进入本轮元数据补全刷新。",
            "items": [
                {"field": "record_id", "label": "record_id", "value": parsed["record_id"], "note": field_note("record_id", parsed["record_id"])},
                {"field": "title", "label": "title", "value": parsed["title"], "note": field_note("title", parsed["title"])},
                {"field": "target_path", "label": "target_path", "value": parsed["target_path"], "note": field_note("target_path", parsed["target_path"])},
                {
                    "field": "missing_fields",
                    "label": "missing_fields",
                    "value": parsed["missing_fields"],
                    "note": f"缺失字段 = {'、'.join(split_csv_values(parsed['missing_fields'])) or '无'}。",
                },
            ],
        },
    }


def annotate_metadata_refresh(message: str) -> dict | None:
    match = METADATA_REFRESH_RE.match(message)
    if not match:
        return None
    parsed = {
        "record_id": match.group("record_id").strip(),
        "title": match.group("title").strip(),
        "id": match.group("id").strip(),
        "provider": match.group("provider").strip(),
        "target_path": match.group("target_path").strip(),
        "updated_fields": match.group("updated_fields").strip(),
    }
    return {
        "kind": "metadata_refresh",
        "parsed": parsed,
        "annotation": {
            "title": "元数据刷新成功",
            "summary": "这条记录已经从资料库重新拉取并补全了部分元数据。",
            "items": [
                {"field": "record_id", "label": "record_id", "value": parsed["record_id"], "note": field_note("record_id", parsed["record_id"])},
                {"field": "title", "label": "title", "value": parsed["title"], "note": field_note("title", parsed["title"])},
                {"field": "id", "label": "id", "value": parsed["id"], "note": field_note("id", parsed["id"])},
                {"field": "provider", "label": "provider", "value": parsed["provider"], "note": field_note("provider", parsed["provider"])},
                {"field": "target_path", "label": "target_path", "value": parsed["target_path"], "note": field_note("target_path", parsed["target_path"])},
                {
                    "field": "updated_fields",
                    "label": "updated_fields",
                    "value": parsed["updated_fields"],
                    "note": f"已补全字段 = {'、'.join(split_csv_values(parsed['updated_fields'])) or '无'}。",
                },
            ],
        },
    }


def annotate_metadata_refresh_fail(message: str) -> dict | None:
    match = METADATA_REFRESH_FAIL_RE.match(message)
    if not match:
        return None
    parsed = {
        "record_id": match.group("record_id").strip(),
        "title": match.group("title").strip(),
        "target_path": match.group("target_path").strip(),
        "reason": match.group("reason").strip(),
    }
    return {
        "kind": "metadata_refresh_failed",
        "parsed": parsed,
        "annotation": {
            "title": "元数据刷新失败",
            "summary": "这条记录进入了元数据刷新，但本次没有成功补全。",
            "items": [
                {"field": "record_id", "label": "record_id", "value": parsed["record_id"], "note": field_note("record_id", parsed["record_id"])},
                {"field": "title", "label": "title", "value": parsed["title"], "note": field_note("title", parsed["title"])},
                {"field": "target_path", "label": "target_path", "value": parsed["target_path"], "note": field_note("target_path", parsed["target_path"])},
                {"field": "reason", "label": "reason", "value": parsed["reason"], "note": field_note("reason", parsed["reason"])},
            ],
        },
    }


def annotate_metadata_scan_done(message: str) -> dict | None:
    match = METADATA_SCAN_DONE_RE.match(message)
    if not match:
        return None
    refreshed = match.group("refreshed").strip()
    total = match.group("total").strip()
    return {
        "kind": "metadata_scan_done",
        "parsed": {"refreshed": refreshed, "total": total},
        "annotation": {
            "title": "元数据巡检完成",
            "summary": f"本轮巡检共处理 {total} 条记录，成功刷新 {refreshed} 条。",
            "items": [
                {"field": "refreshed", "label": "refreshed", "value": refreshed, "note": field_note("refreshed", refreshed)},
                {"field": "total", "label": "total", "value": total, "note": field_note("total", total)},
            ],
        },
    }

