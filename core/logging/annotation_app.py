import re

from .annotation_common import (
    annotate_pid_message,
    annotate_simple,
    annotate_simple_path,
)


DELETE_DIR_RE = re.compile(
    r"^文件系统兑备删除软链接目录:\s*(?P<path>.+)\s+\((?P<reason>[^()]*)\)$"
)
WATCHING_RE = re.compile(r"^Watching:\s*(?P<path>.+)$")
START_RE = re.compile(r"^开始识别:\s*(?P<path>.+)$")
TG_SENT_RE = re.compile(r"^TG 通知已发送:\s*(?P<title>.+)$")
TG_FAIL_RE = re.compile(r"^TG 通知发送失败:\s*(?P<reason>.+)$")
TG_EX_RE = re.compile(r"^TG 通知异常:\s*(?P<reason>.+)$")
EMBY_OK_RE = re.compile(r"^Emby 库扫描已触发（本批次 (?P<count>\d+) 个文件）：(?P<message>.+)$")
EMBY_FAIL_RE = re.compile(r"^Emby 库扫描触发失败：(?P<message>.+)$")
EMBY_SKIP_RE = re.compile(r"^Emby 通知已启用但 URL 或 API Key 未配置，跳过刷新$")
SYNC_DELETE_LINK_RE = re.compile(r"^同步删除软链接:\s*(?P<path>.+?)\s*\((?P<reason>.+)\)$")
SYNC_DELETE_TARGET_RE = re.compile(r"^同步删除目标文件:\s*(?P<path>.+?)\s*\((?P<reason>.+)\)$")
DELETE_LINK_FAIL_RE = re.compile(r"^删除软链接失败\s+(?P<path>.+?):\s*(?P<reason>.+)$")
DELETE_TARGET_FAIL_RE = re.compile(r"^删除目标文件失败\s+(?P<path>.+?):\s*(?P<reason>.+)$")
REBUILD_SCRAPE_RE = re.compile(r"^检测到缺失的刮削产物，准备自动修复:\s*(?P<path>.+)$")
REBUILD_SYMLINK_RE = re.compile(r"^检测到缺失的软链接产物，准备自动重建:\s*(?P<path>.+)$")
SKIP_NFO_RE = re.compile(r"^跳过已有元数据（\.nfo）的文件:\s*(?P<path>.+)$")
SKIP_DECIMAL_RE = re.compile(r"^跳过小数集（总集篇）:\s*(?P<path>.+)$")
FAILED_WATCH_RE = re.compile(r"^Failed to watch\s+(?P<path>.+?):\s*(?P<reason>.+)$")
POLL_ERROR_RE = re.compile(r"^Poll error:\s*(?P<reason>.+)$")
INVALID_HTTP_RE = re.compile(r"^Invalid HTTP request received\.$")
STARTED_SERVER_PROC_RE = re.compile(r"^Started server process \[(?P<pid>\d+)\]$")
FINISHED_SERVER_PROC_RE = re.compile(r"^Finished server process \[(?P<pid>\d+)\]$")
UVICORN_RUNNING_RE = re.compile(r"^Uvicorn running on\s+(?P<addr>.+?)\s+\(Press CTRL\+C to quit\)$")
WS_ACCEPT_RE = re.compile(r'^(?P<client>.+?) - "WebSocket (?P<path>.+)" \[accepted\]$')


def annotate_delete_dir(message: str) -> dict | None:
    match = DELETE_DIR_RE.match(message)
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


def annotate_start(message: str) -> dict | None:
    return annotate_simple_path(
        message,
        START_RE,
        "recognition_start",
        "开始处理",
        "系统已经把这个文件加入识别流程。",
        "当前开始处理的实际文件路径。",
    )


def annotate_watching(message: str) -> dict | None:
    return annotate_simple_path(
        message,
        WATCHING_RE,
        "watching",
        "监控目录加载",
        "Watcher 已经开始监控这个目录。",
        "当前处于监控中的目录路径。",
    )


def annotate_tg_sent(message: str) -> dict | None:
    match = TG_SENT_RE.match(message)
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


def annotate_tg_failure(message: str, exception_mode: bool = False) -> dict | None:
    match = (TG_EX_RE if exception_mode else TG_FAIL_RE).match(message)
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


def annotate_emby_ok(message: str) -> dict | None:
    match = EMBY_OK_RE.match(message)
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


def annotate_emby_fail(message: str) -> dict | None:
    match = EMBY_FAIL_RE.match(message)
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


def annotate_sync_delete(message: str, target_mode: bool = False) -> dict | None:
    match = (SYNC_DELETE_TARGET_RE if target_mode else SYNC_DELETE_LINK_RE).match(message)
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


def annotate_delete_failure(message: str, target_mode: bool = False) -> dict | None:
    match = (DELETE_TARGET_FAIL_RE if target_mode else DELETE_LINK_FAIL_RE).match(message)
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


def annotate_failed_watch(message: str) -> dict | None:
    match = FAILED_WATCH_RE.match(message)
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


def annotate_poll_error(message: str) -> dict | None:
    match = POLL_ERROR_RE.match(message)
    if not match:
        return None
    reason = match.group("reason").strip()
    return {
        "kind": "poll_error",
        "parsed": {"reason": reason},
        "annotation": {
            "title": "轮询扫描异常",
            "summary": "后台轮询扫描监控目录时发生异常。",
            "items": [
                {
                    "field": "reason",
                    "label": "reason",
                    "value": reason,
                    "note": "轮询扫描失败时捕获到的异常信息。",
                },
            ],
        },
    }


def annotate_invalid_http() -> dict:
    return annotate_simple(
        "无效 HTTP 请求",
        "服务收到了一次不符合标准格式的 HTTP 请求，通常来自探测、异常客户端或错误连接。",
        "invalid_http",
    )


def annotate_uvicorn_running(message: str) -> dict | None:
    match = UVICORN_RUNNING_RE.match(message)
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


def annotate_ws_accept(message: str) -> dict | None:
    match = WS_ACCEPT_RE.match(message)
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


def annotate_started_server_process(message: str) -> dict | None:
    return annotate_pid_message(
        message,
        STARTED_SERVER_PROC_RE,
        "server_process_started",
        "服务进程已启动",
        "Uvicorn 主进程已经创建。",
    )


def annotate_finished_server_process(message: str) -> dict | None:
    return annotate_pid_message(
        message,
        FINISHED_SERVER_PROC_RE,
        "server_process_finished",
        "服务进程已退出",
        "Uvicorn 主进程已经结束。",
    )

