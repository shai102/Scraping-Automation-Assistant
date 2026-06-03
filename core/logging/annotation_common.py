import re


FIELD_LABELS = {
    "title": "title",
    "path": "path",
    "name": "name",
    "year": "year",
    "type": "type",
    "season": "season",
    "episode": "episode",
    "provider": "provider",
    "id": "id",
    "record_id": "record_id",
    "matched_title": "matched_title",
    "matched_id": "matched_id",
    "parsed_title": "parsed_title",
    "query_title": "query_title",
    "result": "result",
    "parse_source": "parse_source",
    "resolution": "resolution",
    "source": "source",
    "video_codec": "video_codec",
    "audio_codec": "audio_codec",
    "release_group": "release_group",
    "target_path": "target_path",
    "missing_fields": "missing_fields",
    "updated_fields": "updated_fields",
    "count": "count",
    "refreshed": "refreshed",
    "total": "total",
    "reason": "reason",
}


def parse_pipe_kv(text: str) -> dict:
    parsed = {}
    for segment in str(text or "").split(" | "):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def type_note(value: str) -> str:
    value = str(value or "").strip().lower()
    if value == "movie":
        return "媒体类型 = 电影。"
    if value in {"episode", "tv"}:
        return "媒体类型 = 电视剧。"
    return f"媒体类型 = {value or '未知'}。"


def field_note(field: str, value: str) -> str:
    text = str(value or "").strip()
    if field == "title":
        return f"作品标题 = {text or '空'}"
    if field == "path":
        return "源文件路径 = 软件在处理该文件时，它当时所在的实际物理/网络位置。"
    if field == "name":
        return "规范化文件名 = 系统根据识别结果准备使用的标准名称。"
    if field == "year":
        return f"上映年份 = {text or '未知'}。"
    if field == "type":
        return type_note(text)
    if field == "season":
        return f"季 = 第{text or '?'}季。"
    if field == "episode":
        return f"集 = 第{text or '?'}集。"
    if field == "provider":
        return f"资料库来源 = {text or '未知'}。"
    if field == "id":
        return f"资料库作品 ID = {text or '未知'}。"
    if field == "record_id":
        return f"数据库记录 ID = {text or '未知'}。"
    if field == "matched_title":
        return f"最终匹配标题 = {text or '空'}。"
    if field == "matched_id":
        return f"最终匹配到的资料库 ID = {text or '空'}。"
    if field == "parsed_title":
        return f"从文件名初步解析出的标题 = {text or '空'}。"
    if field == "query_title":
        return f"实际用于搜索资料库的查询标题 = {text or '空'}。"
    if field == "result":
        return f"本次匹配结论 = {text or '空'}。"
    if field == "parse_source":
        return f"识别来源 = {text or '空'}。"
    if field == "resolution":
        return f"分辨率信息 = {text or '空'}。"
    if field == "source":
        return f"片源来源标签 = {text or '空'}。"
    if field == "video_codec":
        return f"视频编码 = {text or '空'}。"
    if field == "audio_codec":
        return f"音频编码 = {text or '空'}。"
    if field == "release_group":
        return f"压制组/发布组 = {text or '空'}。"
    if field == "target_path":
        return "目标文件路径 = 这条记录当前关联的媒体文件实际位置。"
    if field == "missing_fields":
        return f"缺失字段 = {text or '无'}。"
    if field == "updated_fields":
        return f"已补全字段 = {text or '无'}。"
    if field == "count":
        return f"本轮巡检发现的不完整记录数 = {text or '0'}。"
    if field == "refreshed":
        return f"本轮实际成功刷新的记录数 = {text or '0'}。"
    if field == "total":
        return f"本轮尝试处理的记录总数 = {text or '0'}。"
    if field == "reason":
        return f"原因说明 = {text or '空'}。"
    return text or "暂无补充说明。"


def annotation_item(field: str, value: str) -> dict:
    return {
        "field": field,
        "label": FIELD_LABELS.get(field, field),
        "value": str(value or ""),
        "note": field_note(field, value),
    }


def build_kv_annotation(title: str, summary: str, parsed: dict, field_order: list[str]) -> dict:
    items = []
    for field in field_order:
        if field in parsed:
            items.append(annotation_item(field, parsed.get(field, "")))
    return {
        "title": title,
        "summary": summary,
        "items": items,
    }


def annotate_simple(title: str, summary: str, kind: str) -> dict:
    return {
        "kind": kind,
        "parsed": {},
        "annotation": {"title": title, "summary": summary, "items": []},
    }


def annotate_simple_path(message: str, pattern, kind: str, title: str, summary: str, note: str) -> dict | None:
    match = pattern.match(message)
    if not match:
        return None
    path = match.group("path").strip()
    return {
        "kind": kind,
        "parsed": {"path": path},
        "annotation": {
            "title": title,
            "summary": summary,
            "items": [
                {"field": "path", "label": "path", "value": path, "note": note},
            ],
        },
    }


def annotate_pid_message(message: str, pattern, kind: str, title: str, summary: str) -> dict | None:
    match = pattern.match(message)
    if not match:
        return None
    pid = match.group("pid").strip()
    return {
        "kind": kind,
        "parsed": {"pid": pid},
        "annotation": {
            "title": title,
            "summary": summary,
            "items": [
                {"field": "pid", "label": "pid", "value": pid, "note": "当前对应的服务进程号。"},
            ],
        },
    }


def split_csv_values(text: str) -> list[str]:
    return [part.strip() for part in str(text or "").split(",") if part.strip() and part.strip() != "-"]
