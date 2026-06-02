import logging
import os
import re
import sys
from collections import deque
from typing import Optional

from fastapi import APIRouter, Query

from utils.logging_setup import (
    DatePartitionedFileHandler,
    list_available_log_dates,
    normalize_log_kind,
    resolve_log_path,
)

router = APIRouter(prefix="/api/logs", tags=["logs"])

_LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (?P<level>[A-Z]+) - (?P<message>.*)$"
)
_ARCHIVE_RE = re.compile(r"^Archived:\s*(?P<src>.+?)\s*->\s*(?P<dst>.+)$")
_SYMLINK_EXPORT_RE = re.compile(r"^Symlink export:\s*(?P<link>.+?)\s*->\s*(?P<src>.+)$")
_DELETE_TARGET_RE = re.compile(
    r"^链式删除刮削目标:\s*(?P<target>.+?)\s*\(源软链接:\s*(?P<source>.+)\)$"
)
_DELETE_TARGET_DIR_RE = re.compile(
    r"^链式删除刮削目标:\s*(?P<target>.+?)\s*\(源目录软链接:\s*(?P<source>.+)\)$"
)
_DELETE_DIR_RE = re.compile(
    r"^文件系统兑备删除软链接目录:\s*(?P<path>.+)\s+\((?P<reason>[^()]*)\)$"
)
_WATCHING_RE = re.compile(r"^Watching:\s*(?P<path>.+)$")
_START_RE = re.compile(r"^开始识别:\s*(?P<path>.+)$")
_TG_SENT_RE = re.compile(r"^TG 通知已发送:\s*(?P<title>.+)$")
_TG_FAIL_RE = re.compile(r"^TG 通知发送失败:\s*(?P<reason>.+)$")
_TG_EX_RE = re.compile(r"^TG 通知异常:\s*(?P<reason>.+)$")
_EMBY_OK_RE = re.compile(r"^Emby 库扫描已触发（本批次 (?P<count>\d+) 个文件）：(?P<message>.+)$")
_EMBY_FAIL_RE = re.compile(r"^Emby 库扫描触发失败：(?P<message>.+)$")
_EMBY_SKIP_RE = re.compile(r"^Emby 通知已启用但 URL 或 API Key 未配置，跳过刷新$")
_SYNC_DELETE_LINK_RE = re.compile(r"^同步删除软链接:\s*(?P<path>.+?)\s*\((?P<reason>.+)\)$")
_SYNC_DELETE_TARGET_RE = re.compile(r"^同步删除目标文件:\s*(?P<path>.+?)\s*\((?P<reason>.+)\)$")
_DELETE_LINK_FAIL_RE = re.compile(r"^删除软链接失败\s+(?P<path>.+?):\s*(?P<reason>.+)$")
_DELETE_TARGET_FAIL_RE = re.compile(r"^删除目标文件失败\s+(?P<path>.+?):\s*(?P<reason>.+)$")
_REBUILD_SCRAPE_RE = re.compile(r"^检测到缺失的刮削产物，准备自动修复:\s*(?P<path>.+)$")
_REBUILD_SYMLINK_RE = re.compile(r"^检测到缺失的软链接产物，准备自动重建:\s*(?P<path>.+)$")
_SKIP_NFO_RE = re.compile(r"^跳过已有元数据（\.nfo）的文件:\s*(?P<path>.+)$")
_SKIP_DECIMAL_RE = re.compile(r"^跳过小数集（总集篇）:\s*(?P<path>.+)$")
_FAILED_WATCH_RE = re.compile(r"^Failed to watch\s+(?P<path>.+?):\s*(?P<reason>.+)$")
_POLL_ERROR_RE = re.compile(r"^Poll error:\s*(?P<reason>.+)$")
_INVALID_HTTP_RE = re.compile(r"^Invalid HTTP request received\.$")
_STARTED_SERVER_PROC_RE = re.compile(r"^Started server process \[(?P<pid>\d+)\]$")
_FINISHED_SERVER_PROC_RE = re.compile(r"^Finished server process \[(?P<pid>\d+)\]$")
_UVICORN_RUNNING_RE = re.compile(r"^Uvicorn running on\s+(?P<addr>.+?)\s+\(Press CTRL\+C to quit\)$")
_WS_ACCEPT_RE = re.compile(r'^(?P<client>.+?) - "WebSocket (?P<path>.+)" \[accepted\]$')
_METADATA_SCAN_START_RE = re.compile(r"^元数据巡检:\s*发现 (?P<count>\d+) 条不完整记录，开始刷新$")
_METADATA_SCAN_ITEM_RE = re.compile(
    r"^元数据巡检项:\s*record_id=(?P<record_id>\d+)\s+\|\s+title=(?P<title>.*?)\s+\|\s+"
    r"target_path=(?P<target_path>.*?)\s+\|\s+missing_fields=(?P<missing_fields>.+)$"
)
_METADATA_REFRESH_RE = re.compile(
    r"^元数据刷新:\s*record_id=(?P<record_id>\d+)\s+\|\s+title=(?P<title>.*?)\s+\|\s+"
    r"id=(?P<id>.*?)\s+\|\s+provider=(?P<provider>.*?)\s+\|\s+"
    r"target_path=(?P<target_path>.*?)\s+\|\s+updated_fields=(?P<updated_fields>.+)$"
)
_METADATA_REFRESH_FAIL_RE = re.compile(
    r"^元数据刷新失败:\s*record_id=(?P<record_id>\d+)\s+\|\s+title=(?P<title>.*?)\s+\|\s+"
    r"target_path=(?P<target_path>.*?)\s+\|\s+reason=(?P<reason>.+)$"
)
_METADATA_SCAN_DONE_RE = re.compile(r"^元数据巡检完成:\s*刷新了 (?P<refreshed>\d+)/(?P<total>\d+) 条记录$")

_FIELD_LABELS = {
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


def _base_data_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.environ.get("DATA_DIR") or os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )


def _tail_lines(path: str, max_lines: int) -> list[str]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return list(deque(fh, maxlen=max_lines))


def _is_metadata_message(message: str) -> bool:
    text = str(message or "")
    return (
        text.startswith("元数据巡检:")
        or text.startswith("元数据巡检项:")
        or text.startswith("元数据巡检完成:")
        or text.startswith("元数据刷新:")
        or text.startswith("元数据刷新失败:")
    )


def _parse_pipe_kv(text: str) -> dict:
    parsed = {}
    for segment in str(text or "").split(" | "):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _type_note(value: str) -> str:
    value = str(value or "").strip().lower()
    if value == "movie":
        return "媒体类型 = 电影。"
    if value in {"episode", "tv"}:
        return "媒体类型 = 电视剧。"
    return f"媒体类型 = {value or '未知'}。"


def _field_note(field: str, value: str) -> str:
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
        return _type_note(text)
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


def _annotation_item(field: str, value: str) -> dict:
    return {
        "field": field,
        "label": _FIELD_LABELS.get(field, field),
        "value": str(value or ""),
        "note": _field_note(field, value),
    }


def _build_kv_annotation(title: str, summary: str, parsed: dict, field_order: list[str]) -> dict:
    items = []
    for field in field_order:
        if field in parsed:
            items.append(_annotation_item(field, parsed.get(field, "")))
    return {
        "title": title,
        "summary": summary,
        "items": items,
    }


def _annotate_recognition_complete(message: str) -> dict:
    parsed = _parse_pipe_kv(message.split(":", 1)[1])
    summary = f"识别结果 = {parsed.get('type', '未知')}《{parsed.get('title', '未知')}》"
    if parsed.get("year"):
        summary += f"（{parsed['year']}）"
    return {
        "kind": "recognition_complete",
        "parsed": parsed,
        "annotation": _build_kv_annotation(
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


def _annotate_match_result(message: str) -> dict:
    parsed = _parse_pipe_kv(message.split(":", 1)[1])
    summary = (
        f"资料库匹配 = {parsed.get('provider', '未知')} / "
        f"{parsed.get('matched_title', parsed.get('query_title', '未知'))} / "
        f"ID {parsed.get('matched_id', '未知')}"
    )
    return {
        "kind": "match_result",
        "parsed": parsed,
        "annotation": _build_kv_annotation(
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


def _annotate_archive(message: str) -> dict | None:
    match = _ARCHIVE_RE.match(message)
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


def _annotate_delete_target(message: str) -> dict | None:
    match = _DELETE_TARGET_RE.match(message) or _DELETE_TARGET_DIR_RE.match(message)
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


def _annotate_delete_dir(message: str) -> dict | None:
    match = _DELETE_DIR_RE.match(message)
    if not match:
        return None
    path = match.group("path").strip()
    reason = match.group("reason").strip()
    return {
        "kind": "delete_sync_dir",
        "parsed": {"path": path, "reason": reason},
        "annotation": {
            "title": "目录清理信息",
            "summary": "数据库没有对应记录时，系统按路径关系做兜底删除，并清理已经空掉的目录。",
            "items": [
                {"field": "path", "label": "path", "value": path, "note": "被兜底清理的目录路径。"},
                {"field": "reason", "label": "reason", "value": reason, "note": "本次触发兜底删除的原因。"},
            ],
        },
    }


def _annotate_symlink_export(message: str) -> dict | None:
    match = _SYMLINK_EXPORT_RE.match(message)
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


def _annotate_start(message: str) -> dict | None:
    match = _START_RE.match(message)
    if not match:
        return None
    path = match.group("path").strip()
    return {
        "kind": "recognition_start",
        "parsed": {"path": path},
        "annotation": {
            "title": "开始处理",
            "summary": "系统已经把这个文件加入识别流程。",
            "items": [
                {"field": "path", "label": "path", "value": path, "note": "当前开始处理的实际文件路径。"},
            ],
        },
    }


def _annotate_watching(message: str) -> dict | None:
    match = _WATCHING_RE.match(message)
    if not match:
        return None
    path = match.group("path").strip()
    return {
        "kind": "watching",
        "parsed": {"path": path},
        "annotation": {
            "title": "监控目录加载",
            "summary": "Watcher 已经开始监控这个目录。",
            "items": [
                {"field": "path", "label": "path", "value": path, "note": "当前处于监控中的目录路径。"},
            ],
        },
    }


def _annotate_tg_sent(message: str) -> dict | None:
    match = _TG_SENT_RE.match(message)
    if not match:
        return None
    title = match.group("title").strip()
    return {
        "kind": "tg_sent",
        "parsed": {"title": title},
        "annotation": {
            "title": "TG 通知已发送",
            "summary": f"Telegram 通知已经成功发出：{title}",
            "items": [
                {"field": "title", "label": "title", "value": title, "note": "本次通知对应的作品标题。"},
            ],
        },
    }


def _annotate_tg_failure(message: str, exception_mode: bool = False) -> dict | None:
    match = (_TG_EX_RE if exception_mode else _TG_FAIL_RE).match(message)
    if not match:
        return None
    reason = match.group("reason").strip()
    return {
        "kind": "tg_exception" if exception_mode else "tg_failed",
        "parsed": {"reason": reason},
        "annotation": {
            "title": "TG 通知失败",
            "summary": "Telegram 通知没有成功发出。",
            "items": [
                {"field": "reason", "label": "reason", "value": reason, "note": "失败原因或 Telegram 返回信息。"},
            ],
        },
    }


def _annotate_emby_ok(message: str) -> dict | None:
    match = _EMBY_OK_RE.match(message)
    if not match:
        return None
    count = match.group("count").strip()
    text = match.group("message").strip()
    return {
        "kind": "emby_refresh_ok",
        "parsed": {"count": count, "message": text},
        "annotation": {
            "title": "Emby/Jellyfin 入库通知",
            "summary": f"媒体库刷新已经触发，本批次涉及 {count} 个文件。",
            "items": [
                {"field": "count", "label": "count", "value": count, "note": "本次合并触发入库扫描的文件数量。"},
                {"field": "message", "label": "message", "value": text, "note": "Emby/Jellyfin 返回的结果描述。"},
            ],
        },
    }


def _annotate_emby_fail(message: str) -> dict | None:
    match = _EMBY_FAIL_RE.match(message)
    if not match:
        return None
    text = match.group("message").strip()
    return {
        "kind": "emby_refresh_failed",
        "parsed": {"message": text},
        "annotation": {
            "title": "Emby/Jellyfin 入库失败",
            "summary": "媒体库扫描触发失败。",
            "items": [
                {"field": "message", "label": "message", "value": text, "note": "服务端返回的失败原因。"},
            ],
        },
    }


def _annotate_sync_delete(message: str, target_mode: bool = False) -> dict | None:
    match = (_SYNC_DELETE_TARGET_RE if target_mode else _SYNC_DELETE_LINK_RE).match(message)
    if not match:
        return None
    path = match.group("path").strip()
    reason = match.group("reason").strip()
    return {
        "kind": "sync_delete_target" if target_mode else "sync_delete_link",
        "parsed": {"path": path, "reason": reason},
        "annotation": {
            "title": "同步删除信息",
            "summary": "上游文件/目录删除后，系统正在同步清理对应产物。",
            "items": [
                {"field": "path", "label": "path", "value": path, "note": "本次被同步删除的目标路径。"},
                {"field": "reason", "label": "reason", "value": reason, "note": "触发本次同步删除的来源说明。"},
            ],
        },
    }


def _annotate_delete_failure(message: str, target_mode: bool = False) -> dict | None:
    match = (_DELETE_TARGET_FAIL_RE if target_mode else _DELETE_LINK_FAIL_RE).match(message)
    if not match:
        return None
    path = match.group("path").strip()
    reason = match.group("reason").strip()
    return {
        "kind": "sync_delete_target_failed" if target_mode else "sync_delete_link_failed",
        "parsed": {"path": path, "reason": reason},
        "annotation": {
            "title": "同步删除失败",
            "summary": "系统试图清理同步删除目标时失败。",
            "items": [
                {"field": "path", "label": "path", "value": path, "note": "删除失败的目标路径。"},
                {"field": "reason", "label": "reason", "value": reason, "note": "本次删除失败的异常信息。"},
            ],
        },
    }


def _annotate_simple_path(message: str, pattern, kind: str, title: str, summary: str, note: str) -> dict | None:
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


def _annotate_failed_watch(message: str) -> dict | None:
    match = _FAILED_WATCH_RE.match(message)
    if not match:
        return None
    path = match.group("path").strip()
    reason = match.group("reason").strip()
    return {
        "kind": "watch_failed",
        "parsed": {"path": path, "reason": reason},
        "annotation": {
            "title": "监控目录加载失败",
            "summary": "Watcher 无法监控这个目录。",
            "items": [
                {"field": "path", "label": "path", "value": path, "note": "加载失败的监控目录。"},
                {"field": "reason", "label": "reason", "value": reason, "note": "无法监控该目录的原因。"},
            ],
        },
    }


def _annotate_pid_message(message: str, pattern, kind: str, title: str, summary: str) -> dict | None:
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


def _annotate_uvicorn_running(message: str) -> dict | None:
    match = _UVICORN_RUNNING_RE.match(message)
    if not match:
        return None
    addr = match.group("addr").strip()
    return {
        "kind": "uvicorn_running",
        "parsed": {"addr": addr},
        "annotation": {
            "title": "Web 服务监听中",
            "summary": "Uvicorn 已经开始监听 HTTP 服务。",
            "items": [
                {"field": "addr", "label": "addr", "value": addr, "note": "当前对外监听的地址和端口。"},
            ],
        },
    }


def _annotate_ws_accept(message: str) -> dict | None:
    match = _WS_ACCEPT_RE.match(message)
    if not match:
        return None
    client = match.group("client").strip()
    path = match.group("path").strip()
    return {
        "kind": "ws_accept",
        "parsed": {"client": client, "path": path},
        "annotation": {
            "title": "WebSocket 连接建立",
            "summary": "前端页面已经成功连上实时推送通道。",
            "items": [
                {"field": "client", "label": "client", "value": client, "note": "发起本次连接的客户端地址。"},
                {"field": "path", "label": "path", "value": path, "note": "建立连接的 WebSocket 路径。"},
            ],
        },
    }


def _annotate_invalid_http() -> dict:
    return {
        "kind": "invalid_http",
        "parsed": {},
        "annotation": {
            "title": "无效 HTTP 请求",
            "summary": "服务收到了一次不符合标准格式的 HTTP 请求，通常来自探测、异常客户端或错误连接。",
            "items": [],
        },
    }


def _annotate_simple(title: str, summary: str, kind: str) -> dict:
    return {
        "kind": kind,
        "parsed": {},
        "annotation": {"title": title, "summary": summary, "items": []},
    }


def _split_csv_values(text: str) -> list[str]:
    return [part.strip() for part in str(text or "").split(",") if part.strip() and part.strip() != "-"]


def _annotate_metadata_scan_start(message: str) -> dict | None:
    match = _METADATA_SCAN_START_RE.match(message)
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
                {"field": "count", "label": "count", "value": count, "note": _field_note("count", count)},
            ],
        },
    }


def _annotate_metadata_scan_item(message: str) -> dict | None:
    match = _METADATA_SCAN_ITEM_RE.match(message)
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
                {"field": "record_id", "label": "record_id", "value": parsed["record_id"], "note": _field_note("record_id", parsed["record_id"])},
                {"field": "title", "label": "title", "value": parsed["title"], "note": _field_note("title", parsed["title"])},
                {"field": "target_path", "label": "target_path", "value": parsed["target_path"], "note": _field_note("target_path", parsed["target_path"])},
                {"field": "missing_fields", "label": "missing_fields", "value": parsed["missing_fields"], "note": f"缺失字段 = {'、'.join(_split_csv_values(parsed['missing_fields'])) or '无'}。"},
            ],
        },
    }


def _annotate_metadata_refresh(message: str) -> dict | None:
    match = _METADATA_REFRESH_RE.match(message)
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
                {"field": "record_id", "label": "record_id", "value": parsed["record_id"], "note": _field_note("record_id", parsed["record_id"])},
                {"field": "title", "label": "title", "value": parsed["title"], "note": _field_note("title", parsed["title"])},
                {"field": "id", "label": "id", "value": parsed["id"], "note": _field_note("id", parsed["id"])},
                {"field": "provider", "label": "provider", "value": parsed["provider"], "note": _field_note("provider", parsed["provider"])},
                {"field": "target_path", "label": "target_path", "value": parsed["target_path"], "note": _field_note("target_path", parsed["target_path"])},
                {"field": "updated_fields", "label": "updated_fields", "value": parsed["updated_fields"], "note": f"已补全字段 = {'、'.join(_split_csv_values(parsed['updated_fields'])) or '无'}。"},
            ],
        },
    }


def _annotate_metadata_refresh_fail(message: str) -> dict | None:
    match = _METADATA_REFRESH_FAIL_RE.match(message)
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
                {"field": "record_id", "label": "record_id", "value": parsed["record_id"], "note": _field_note("record_id", parsed["record_id"])},
                {"field": "title", "label": "title", "value": parsed["title"], "note": _field_note("title", parsed["title"])},
                {"field": "target_path", "label": "target_path", "value": parsed["target_path"], "note": _field_note("target_path", parsed["target_path"])},
                {"field": "reason", "label": "reason", "value": parsed["reason"], "note": _field_note("reason", parsed["reason"])},
            ],
        },
    }


def _annotate_metadata_scan_done(message: str) -> dict | None:
    match = _METADATA_SCAN_DONE_RE.match(message)
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
                {"field": "refreshed", "label": "refreshed", "value": refreshed, "note": _field_note("refreshed", refreshed)},
                {"field": "total", "label": "total", "value": total, "note": _field_note("total", total)},
            ],
        },
    }


def _analyze_log_message(message: str) -> dict:
    if message.startswith("元数据巡检:"):
        annotated = _annotate_metadata_scan_start(message)
        if annotated:
            return annotated
    if message.startswith("元数据巡检项:"):
        annotated = _annotate_metadata_scan_item(message)
        if annotated:
            return annotated
    if message.startswith("元数据刷新:"):
        annotated = _annotate_metadata_refresh(message)
        if annotated:
            return annotated
    if message.startswith("元数据刷新失败:"):
        annotated = _annotate_metadata_refresh_fail(message)
        if annotated:
            return annotated
    if message.startswith("元数据巡检完成:"):
        annotated = _annotate_metadata_scan_done(message)
        if annotated:
            return annotated
    if message.startswith("识别完成:"):
        return _annotate_recognition_complete(message)
    if message.startswith("资料库匹配:"):
        return _annotate_match_result(message)
    if message.startswith("Archived:"):
        annotated = _annotate_archive(message)
        if annotated:
            return annotated
    if message.startswith("链式删除刮削目标:"):
        annotated = _annotate_delete_target(message)
        if annotated:
            return annotated
    if message.startswith("文件系统兑备删除软链接目录:"):
        annotated = _annotate_delete_dir(message)
        if annotated:
            return annotated
    if message.startswith("Symlink export:"):
        annotated = _annotate_symlink_export(message)
        if annotated:
            return annotated
    if message.startswith("开始识别:"):
        annotated = _annotate_start(message)
        if annotated:
            return annotated
    if message.startswith("Watching:"):
        annotated = _annotate_watching(message)
        if annotated:
            return annotated
    if message.startswith("TG 通知已发送:"):
        annotated = _annotate_tg_sent(message)
        if annotated:
            return annotated
    if message.startswith("TG 通知发送失败:"):
        annotated = _annotate_tg_failure(message, exception_mode=False)
        if annotated:
            return annotated
    if message.startswith("TG 通知异常:"):
        annotated = _annotate_tg_failure(message, exception_mode=True)
        if annotated:
            return annotated
    if message.startswith("Emby 库扫描已触发"):
        annotated = _annotate_emby_ok(message)
        if annotated:
            return annotated
    if message.startswith("Emby 库扫描触发失败："):
        annotated = _annotate_emby_fail(message)
        if annotated:
            return annotated
    if _EMBY_SKIP_RE.match(message):
        return _annotate_simple("Emby/Jellyfin 通知已跳过", "入库通知已启用，但 URL 或 API Key 没配置，所以这次没有发起刷新。", "emby_refresh_skipped")
    if message.startswith("同步删除软链接:"):
        annotated = _annotate_sync_delete(message, target_mode=False)
        if annotated:
            return annotated
    if message.startswith("同步删除目标文件:"):
        annotated = _annotate_sync_delete(message, target_mode=True)
        if annotated:
            return annotated
    if message.startswith("删除软链接失败 "):
        annotated = _annotate_delete_failure(message, target_mode=False)
        if annotated:
            return annotated
    if message.startswith("删除目标文件失败 "):
        annotated = _annotate_delete_failure(message, target_mode=True)
        if annotated:
            return annotated
    if message.startswith("检测到缺失的刮削产物，准备自动修复:"):
        annotated = _annotate_simple_path(
            message,
            _REBUILD_SCRAPE_RE,
            "rebuild_scrape_target",
            "刮削产物自动修复",
            "系统检测到归档结果缺失，准备自动补建。",
            "即将自动修复的刮削目标路径。",
        )
        if annotated:
            return annotated
    if message.startswith("检测到缺失的软链接产物，准备自动重建:"):
        annotated = _annotate_simple_path(
            message,
            _REBUILD_SYMLINK_RE,
            "rebuild_symlink_target",
            "软链接产物自动重建",
            "系统检测到导出软链接缺失，准备自动重建。",
            "即将自动重建的软链接源路径。",
        )
        if annotated:
            return annotated
    if message.startswith("跳过已有元数据（.nfo）的文件:"):
        annotated = _annotate_simple_path(
            message,
            _SKIP_NFO_RE,
            "skip_nfo",
            "跳过已刮削文件",
            "该文件旁边已经存在元数据文件，所以本次没有重复刮削。",
            "被跳过处理的文件路径。",
        )
        if annotated:
            return annotated
    if message.startswith("跳过小数集（总集篇）:"):
        annotated = _annotate_simple_path(
            message,
            _SKIP_DECIMAL_RE,
            "skip_decimal_episode",
            "跳过特殊集格式",
            "系统识别到这是小数集/总集篇命名，按当前规则跳过处理。",
            "被跳过的文件路径。",
        )
        if annotated:
            return annotated
    if message.startswith("Failed to watch "):
        annotated = _annotate_failed_watch(message)
        if annotated:
            return annotated
    if message.startswith("Poll error:"):
        match = _POLL_ERROR_RE.match(message)
        if match:
            return {
                "kind": "poll_error",
                "parsed": {"reason": match.group("reason").strip()},
                "annotation": {
                    "title": "轮询扫描异常",
                    "summary": "后台轮询扫描监控目录时发生异常。",
                    "items": [
                        {"field": "reason", "label": "reason", "value": match.group("reason").strip(), "note": "轮询扫描失败时捕获到的异常信息。"},
                    ],
                },
            }
    if _INVALID_HTTP_RE.match(message):
        return _annotate_invalid_http()
    annotated = _annotate_pid_message(
        message,
        _STARTED_SERVER_PROC_RE,
        "server_process_started",
        "服务进程已启动",
        "Uvicorn 主进程已经创建。",
    )
    if annotated:
        return annotated
    annotated = _annotate_pid_message(
        message,
        _FINISHED_SERVER_PROC_RE,
        "server_process_finished",
        "服务进程已退出",
        "Uvicorn 主进程已经结束。",
    )
    if annotated:
        return annotated
    annotated = _annotate_uvicorn_running(message)
    if annotated:
        return annotated
    annotated = _annotate_ws_accept(message)
    if annotated:
        return annotated
    if message == "FolderWatcher started":
        return _annotate_simple("监控器启动", "目录监控线程已经启动。", "watcher_started")
    if message == "FolderWatcher stopped":
        return _annotate_simple("监控器停止", "目录监控线程已经停止。", "watcher_stopped")
    if message == "Server stopped":
        return _annotate_simple("服务停止", "Web 服务与监控器已经停止。", "server_stopped")
    if message == "Server started — watcher active":
        return _annotate_simple("服务启动", "Web 服务启动完成，Watcher 已进入工作状态。", "server_started")
    if message == "Application startup complete.":
        return _annotate_simple("应用启动完成", "FastAPI 应用初始化已经完成。", "application_startup_complete")
    if message == "Waiting for application startup.":
        return _annotate_simple("等待应用启动", "服务进程已经起来，正在执行应用初始化。", "application_startup_wait")
    if message == "Shutting down":
        return _annotate_simple("服务准备关闭", "服务已收到关闭信号，准备停止。", "server_shutting_down")
    if message == "Waiting for application shutdown.":
        return _annotate_simple("等待应用关闭", "服务正在执行关闭前的清理逻辑。", "application_shutdown_wait")
    if message == "Application shutdown complete.":
        return _annotate_simple("应用关闭完成", "FastAPI 应用关闭流程已经完成。", "application_shutdown_complete")
    if message == "connection open":
        return _annotate_simple("实时连接已打开", "有一个 WebSocket 实时推送连接已建立。", "ws_open")
    if message == "connection closed":
        return _annotate_simple("实时连接已关闭", "一个 WebSocket 实时推送连接已经断开。", "ws_closed")
    return {"kind": "general", "parsed": {}, "annotation": None}


@router.get("")
def read_logs(
    limit: int = Query(200, ge=20, le=1000),
    level: str = Query("", description="INFO / WARNING / ERROR"),
    keyword: Optional[str] = Query(None),
    kind: str = Query("scrape", description="scrape / app / metadata"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
):
    data_dir = _base_data_dir()
    log_kind = normalize_log_kind(kind)
    log_path = resolve_log_path(data_dir, log_kind, date)
    raw_lines = _tail_lines(log_path, max(limit * 10, 500))
    want_level = str(level or "").strip().upper()
    want_keyword = str(keyword or "").strip().lower()
    available_dates = list_available_log_dates(data_dir, log_kind)
    selected_date = os.path.splitext(os.path.basename(log_path))[0]

    items = []
    for raw in reversed(raw_lines):
        line = raw.rstrip("\r\n")
        if not line:
            continue

        match = _LOG_LINE_RE.match(line)
        if match:
            entry = {
                "timestamp": match.group("ts"),
                "level": match.group("level"),
                "message": match.group("message"),
                "raw": line,
            }
        else:
            entry = {
                "timestamp": "",
                "level": "INFO",
                "message": line,
                "raw": line,
            }

        if log_kind == "scrape" and _is_metadata_message(entry["message"]):
            continue

        if want_level and entry["level"] != want_level:
            continue
        if want_keyword and want_keyword not in entry["raw"].lower():
            continue

        analyzed = _analyze_log_message(entry["message"])
        entry["kind"] = analyzed.get("kind", "general")
        entry["parsed"] = analyzed.get("parsed", {})
        entry["annotation"] = analyzed.get("annotation")
        items.append(entry)
        if len(items) >= limit:
            break

    return {
        "path": log_path,
        "exists": os.path.isfile(log_path),
        "kind": log_kind,
        "selected_date": selected_date,
        "available_dates": available_dates,
        "items": items,
    }


@router.delete("")
def clear_logs(
    kind: str = Query("scrape", description="scrape / app / metadata"),
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
):
    data_dir = _base_data_dir()
    log_kind = normalize_log_kind(kind)
    log_path = resolve_log_path(data_dir, log_kind, date)
    if not os.path.isfile(log_path):
        return {"ok": True, "message": "日志文件不存在，无需清除"}
    try:
        norm = os.path.normcase(os.path.normpath(log_path))
        truncated = False
        for logger_name in list(logging.Logger.manager.loggerDict) + [None]:
            log_obj = logging.getLogger(logger_name)
            for handler in list(getattr(log_obj, "handlers", [])):
                if isinstance(handler, DatePartitionedFileHandler):
                    handler.truncate_path(log_path)
                    truncated = True
                elif isinstance(handler, logging.FileHandler):
                    try:
                        handler_path = os.path.normcase(
                            os.path.normpath(handler.baseFilename)
                        )
                    except Exception:
                        continue
                    if handler_path == norm:
                        handler.close()
                        handler.stream = open(
                            handler.baseFilename,
                            handler.mode,
                            encoding=handler.encoding,
                        )
                        handler.stream.seek(0)
                        handler.stream.truncate(0)
                        handler.stream.flush()
                        truncated = True
        if not truncated:
            with open(log_path, "w", encoding="utf-8") as fh:
                fh.truncate(0)
        return {"ok": True, "message": "日志已清除"}
    except Exception as err:
        return {"ok": False, "message": f"清除失败: {err}"}
