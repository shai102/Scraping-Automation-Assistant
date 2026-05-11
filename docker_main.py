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


def _setup_logging(data_dir: str) -> None:
    log_path = os.path.join(data_dir, "media_renamer.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    # Console — INFO and above (Docker collects stdout/stderr)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    root_logger.addHandler(console_handler)

    # File — ERROR and above only
    try:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        root_logger.addHandler(file_handler)
    except OSError as e:
        root_logger.warning("Could not open log file %s: %s", log_path, e)


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
