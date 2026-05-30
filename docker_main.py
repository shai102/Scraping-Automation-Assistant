"""Docker entry point — runs uvicorn directly without pystray or webbrowser.

Usage:
    python docker_main.py

Environment variables:
    DATA_DIR   Path for persistent data (DB, config, cache, logs).
               Defaults to the project root when not set.
               In Docker, set to the mounted volume path (e.g. /data).
    PORT       HTTP port to listen on. Defaults to 8090.
    TZ         Timezone, e.g. Asia/Shanghai.
"""

import logging
import os
import sys


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8090))


def _resolve_data_dir() -> str:
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)
        return data_dir
    return os.path.dirname(os.path.abspath(__file__))


class _ScrapeLogFilter(logging.Filter):
    """Keep scrape-related process logs in a dedicated log file."""

    _PREFIXES = (
        "monitor.watcher",
        "api.routes.records",
        "core.workers.task_runner",
        "core.workers.execution_runner",
        "core.services.worker_context",
    )
    _MESSAGE_MARKERS = (
        "开始识别:",
        "识别完成:",
        "识别失败:",
        "资料库匹配:",
        "资料库匹配失败:",
        "NFO fast-path:",
        "Archived:",
        "Archive failed",
        "Recognition failed",
        "检测到缺失的刮削产物",
        "跳过已有元数据",
        "恢复:",
        "Restored:",
        "元数据巡检:",
        "元数据刷新:",
    )

    def filter(self, record):
        if record.levelno < logging.INFO:
            return False
        if not any(str(record.name or "").startswith(p) for p in self._PREFIXES):
            return False
        message = str(record.getMessage() or "")
        return any(marker in message for marker in self._MESSAGE_MARKERS)


def _setup_logging(data_dir: str) -> None:
    log_path = os.path.join(data_dir, "media_renamer.log")
    scrape_log_path = os.path.join(data_dir, "scrape_process.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console — INFO and above (Docker collects stdout/stderr)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)
    root_logger.addHandler(console_handler)

    # File — general application log (ERROR and above)
    try:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(fmt)
        root_logger.addHandler(file_handler)
    except OSError as e:
        root_logger.warning("Could not open log file %s: %s", log_path, e)

    # File — scrape process log (INFO, filtered to scrape-related messages)
    try:
        scrape_handler = logging.FileHandler(scrape_log_path, encoding="utf-8")
        scrape_handler.setLevel(logging.INFO)
        scrape_handler.addFilter(_ScrapeLogFilter())
        scrape_handler.setFormatter(fmt)
        root_logger.addHandler(scrape_handler)
    except OSError as e:
        root_logger.warning("Could not open scrape log file %s: %s", scrape_log_path, e)


def main() -> None:
    data_dir = _resolve_data_dir()
    _setup_logging(data_dir)

    print(f"\n  刮削助手 (Docker Mode)")
    print(f"  DATA_DIR : {data_dir}")
    print(f"  Web 管理界面: http://0.0.0.0:{PORT}\n")

    import uvicorn

    uvicorn.run(
        "server:app",
        host=HOST,
        port=PORT,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
