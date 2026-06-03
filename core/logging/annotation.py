from .annotation_app import (
    EMBY_SKIP_RE,
    INVALID_HTTP_RE,
    annotate_delete_dir,
    annotate_delete_failure,
    annotate_emby_fail,
    annotate_emby_ok,
    annotate_failed_watch,
    annotate_finished_server_process,
    annotate_invalid_http,
    annotate_poll_error,
    annotate_start,
    annotate_started_server_process,
    annotate_sync_delete,
    annotate_tg_failure,
    annotate_tg_sent,
    annotate_uvicorn_running,
    annotate_watching,
    annotate_ws_accept,
)
from .annotation_common import annotate_simple, annotate_simple_path
from .annotation_metadata import (
    annotate_metadata_refresh,
    annotate_metadata_refresh_fail,
    annotate_metadata_scan_done,
    annotate_metadata_scan_item,
    annotate_metadata_scan_start,
)
from .annotation_scrape import (
    annotate_archive,
    annotate_delete_target,
    annotate_match_result,
    annotate_recognition_complete,
    annotate_symlink_export,
)
from .annotation_app import REBUILD_SCRAPE_RE, REBUILD_SYMLINK_RE, SKIP_DECIMAL_RE, SKIP_NFO_RE


def analyze_log_message(message: str) -> dict:
    if message.startswith("元数据巡检:"):
        annotated = annotate_metadata_scan_start(message)
        if annotated:
            return annotated
    if message.startswith("元数据巡检项:"):
        annotated = annotate_metadata_scan_item(message)
        if annotated:
            return annotated
    if message.startswith("元数据刷新:"):
        annotated = annotate_metadata_refresh(message)
        if annotated:
            return annotated
    if message.startswith("元数据刷新失败:"):
        annotated = annotate_metadata_refresh_fail(message)
        if annotated:
            return annotated
    if message.startswith("元数据巡检完成:"):
        annotated = annotate_metadata_scan_done(message)
        if annotated:
            return annotated
    if message.startswith("识别完成:"):
        return annotate_recognition_complete(message)
    if message.startswith("资料库匹配:"):
        return annotate_match_result(message)
    if message.startswith("Archived:"):
        annotated = annotate_archive(message)
        if annotated:
            return annotated
    if message.startswith("链式删除刮削目标:"):
        annotated = annotate_delete_target(message)
        if annotated:
            return annotated
    if message.startswith("文件系统兑备删除软链接目录:"):
        annotated = annotate_delete_dir(message)
        if annotated:
            return annotated
    if message.startswith("Symlink export:"):
        annotated = annotate_symlink_export(message)
        if annotated:
            return annotated
    if message.startswith("开始识别:"):
        annotated = annotate_start(message)
        if annotated:
            return annotated
    if message.startswith("Watching:"):
        annotated = annotate_watching(message)
        if annotated:
            return annotated
    if message.startswith("TG 通知已发送:"):
        annotated = annotate_tg_sent(message)
        if annotated:
            return annotated
    if message.startswith("TG 通知发送失败:"):
        annotated = annotate_tg_failure(message, exception_mode=False)
        if annotated:
            return annotated
    if message.startswith("TG 通知异常:"):
        annotated = annotate_tg_failure(message, exception_mode=True)
        if annotated:
            return annotated
    if message.startswith("Emby 库扫描已触发"):
        annotated = annotate_emby_ok(message)
        if annotated:
            return annotated
    if message.startswith("Emby 库扫描触发失败："):
        annotated = annotate_emby_fail(message)
        if annotated:
            return annotated
    if EMBY_SKIP_RE.match(message):
        return annotate_simple(
            "Emby/Jellyfin 通知已跳过",
            "入库通知已启用，但 URL 或 API Key 没配置，所以这次没有发起刷新。",
            "emby_refresh_skipped",
        )
    if message.startswith("同步删除软链接:"):
        annotated = annotate_sync_delete(message, target_mode=False)
        if annotated:
            return annotated
    if message.startswith("同步删除目标文件:"):
        annotated = annotate_sync_delete(message, target_mode=True)
        if annotated:
            return annotated
    if message.startswith("删除软链接失败 "):
        annotated = annotate_delete_failure(message, target_mode=False)
        if annotated:
            return annotated
    if message.startswith("删除目标文件失败 "):
        annotated = annotate_delete_failure(message, target_mode=True)
        if annotated:
            return annotated
    if message.startswith("检测到缺失的刮削产物，准备自动修复:"):
        annotated = annotate_simple_path(
            message,
            REBUILD_SCRAPE_RE,
            "rebuild_scrape_target",
            "刮削产物自动修复",
            "系统检测到归档结果缺失，准备自动补建。",
            "即将自动修复的刮削目标路径。",
        )
        if annotated:
            return annotated
    if message.startswith("检测到缺失的软链接产物，准备自动重建:"):
        annotated = annotate_simple_path(
            message,
            REBUILD_SYMLINK_RE,
            "rebuild_symlink_target",
            "软链接产物自动重建",
            "系统检测到导出软链接缺失，准备自动重建。",
            "即将自动重建的软链接源路径。",
        )
        if annotated:
            return annotated
    if message.startswith("跳过已有元数据（.nfo）的文件:"):
        annotated = annotate_simple_path(
            message,
            SKIP_NFO_RE,
            "skip_nfo",
            "跳过已刮削文件",
            "该文件旁边已经存在元数据文件，所以本次没有重复刮削。",
            "被跳过处理的文件路径。",
        )
        if annotated:
            return annotated
    if message.startswith("跳过小数集（总集篇）:"):
        annotated = annotate_simple_path(
            message,
            SKIP_DECIMAL_RE,
            "skip_decimal_episode",
            "跳过特殊集格式",
            "系统识别到这是小数集/总集篇命名，按当前规则跳过处理。",
            "被跳过的文件路径。",
        )
        if annotated:
            return annotated
    if message.startswith("Failed to watch "):
        annotated = annotate_failed_watch(message)
        if annotated:
            return annotated
    if message.startswith("Poll error:"):
        annotated = annotate_poll_error(message)
        if annotated:
            return annotated
    if INVALID_HTTP_RE.match(message):
        return annotate_invalid_http()
    annotated = annotate_started_server_process(message)
    if annotated:
        return annotated
    annotated = annotate_finished_server_process(message)
    if annotated:
        return annotated
    annotated = annotate_uvicorn_running(message)
    if annotated:
        return annotated
    annotated = annotate_ws_accept(message)
    if annotated:
        return annotated
    if message == "FolderWatcher started":
        return annotate_simple("监控器启动", "目录监控线程已经启动。", "watcher_started")
    if message == "FolderWatcher stopped":
        return annotate_simple("监控器停止", "目录监控线程已经停止。", "watcher_stopped")
    if message == "Server stopped":
        return annotate_simple("服务停止", "Web 服务与监控器已经停止。", "server_stopped")
    if message == "Server started — watcher active":
        return annotate_simple("服务启动", "Web 服务启动完成，Watcher 已进入工作状态。", "server_started")
    if message == "Application startup complete.":
        return annotate_simple("应用启动完成", "FastAPI 应用初始化已经完成。", "application_startup_complete")
    if message == "Waiting for application startup.":
        return annotate_simple("等待应用启动", "服务进程已经起来，正在执行应用初始化。", "application_startup_wait")
    if message == "Shutting down":
        return annotate_simple("服务准备关闭", "服务已收到关闭信号，准备停止。", "server_shutting_down")
    if message == "Waiting for application shutdown.":
        return annotate_simple("等待应用关闭", "服务正在执行关闭前的清理逻辑。", "application_shutdown_wait")
    if message == "Application shutdown complete.":
        return annotate_simple("应用关闭完成", "FastAPI 应用关闭流程已经完成。", "application_shutdown_complete")
    if message == "connection open":
        return annotate_simple("实时连接已打开", "有一个 WebSocket 实时推送连接已建立。", "ws_open")
    if message == "connection closed":
        return annotate_simple("实时连接已关闭", "一个 WebSocket 实时推送连接已经断开。", "ws_closed")
    return {"kind": "general", "parsed": {}, "annotation": None}
