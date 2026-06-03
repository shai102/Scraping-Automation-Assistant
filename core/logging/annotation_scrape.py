import re

from .annotation_common import build_kv_annotation, parse_pipe_kv


ARCHIVE_RE = re.compile(r"^Archived:\s*(?P<src>.+?)\s*->\s*(?P<dst>.+)$")
SYMLINK_EXPORT_RE = re.compile(r"^Symlink export:\s*(?P<link>.+?)\s*->\s*(?P<src>.+)$")
DELETE_TARGET_RE = re.compile(
    r"^链式删除刮削目标:\s*(?P<target>.+?)\s*\(源软链接:\s*(?P<source>.+)\)$"
)
DELETE_TARGET_DIR_RE = re.compile(
    r"^链式删除刮削目标:\s*(?P<target>.+?)\s*\(源目录软链接:\s*(?P<source>.+)\)$"
)


def annotate_recognition_complete(message: str) -> dict:
    parsed = parse_pipe_kv(message.split(":", 1)[1])
    summary = f"识别结果 = {parsed.get('type', '未知')}《{parsed.get('title', '未知')}》"
    if parsed.get("year"):
        summary += f"（{parsed['year']}）"
    return {
        "kind": "recognition_complete",
        "parsed": parsed,
        "annotation": build_kv_annotation(
            "核心识别信息",
            summary,
            parsed,
            [
                "title",
                "path",
                "name",
                "year",
                "type",
                "season",
                "episode",
                "provider",
                "id",
                "parse_source",
            ],
        ),
    }


def annotate_match_result(message: str) -> dict:
    parsed = parse_pipe_kv(message.split(":", 1)[1])
    summary = (
        f"资料库匹配 = {parsed.get('provider', '未知')} / "
        f"{parsed.get('matched_title', parsed.get('query_title', '未知'))} / "
        f"ID {parsed.get('matched_id', '未知')}"
    )
    return {
        "kind": "match_result",
        "parsed": parsed,
        "annotation": build_kv_annotation(
            "资料库匹配信息",
            summary,
            parsed,
            [
                "parsed_title",
                "query_title",
                "matched_title",
                "matched_id",
                "provider",
                "result",
                "path",
            ],
        ),
    }


def annotate_archive(message: str) -> dict | None:
    match = ARCHIVE_RE.match(message)
    if not match:
        return None
    src = match.group("src").strip()
    dst = match.group("dst").strip()
    return {
        "kind": "archive",
        "parsed": {"source": src, "target": dst},
        "annotation": {
            "title": "归档完成",
            "summary": "文件已经从当前处理路径整理到目标位置。",
            "items": [
                {"field": "source", "label": "source", "value": src, "note": "归档前的处理路径。"},
                {"field": "target", "label": "target", "value": dst, "note": "归档后的目标路径。"},
            ],
        },
    }


def annotate_delete_target(message: str) -> dict | None:
    match = DELETE_TARGET_RE.match(message) or DELETE_TARGET_DIR_RE.match(message)
    if not match:
        return None
    target = match.group("target").strip()
    source = match.group("source").strip()
    return {
        "kind": "delete_sync_target",
        "parsed": {"target": target, "source": source},
        "annotation": {
            "title": "删除同步信息",
            "summary": "检测到上游软链接已删除，正在继续清理下游已经整理出来的目标文件。",
            "items": [
                {"field": "target", "label": "target", "value": target, "note": "需要被同步删除的下游整理结果。"},
                {"field": "source", "label": "source", "value": source, "note": "触发本次链式删除的源软链接路径。"},
            ],
        },
    }


def annotate_symlink_export(message: str) -> dict | None:
    match = SYMLINK_EXPORT_RE.match(message)
    if not match:
        return None
    link = match.group("link").strip()
    src = match.group("src").strip()
    return {
        "kind": "symlink_export",
        "parsed": {"link": link, "source": src},
        "annotation": {
            "title": "软链接导出信息",
            "summary": "系统已将上游文件导出到监控链路的下一跳目录。",
            "items": [
                {"field": "link", "label": "link", "value": link, "note": "导出的软链接/复制目标路径。"},
                {"field": "source", "label": "source", "value": src, "note": "原始文件所在路径。"},
            ],
        },
    }
