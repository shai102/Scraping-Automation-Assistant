"""Checks that a newly detected file has stopped changing before processing."""

import os
import time


TEMP_SUFFIXES = (".part", ".tmp", ".download", ".crdownload", ".aria2")


def file_snapshot(path: str):
    stat = os.stat(path)
    return stat.st_size, stat.st_mtime_ns


def wait_for_file_stable(path: str, *, checks: int = 2, interval_seconds: float = 1.0) -> tuple[bool, str]:
    if str(path).lower().endswith(TEMP_SUFFIXES):
        return False, "下载临时文件尚未完成"
    checks = max(1, min(10, int(checks)))
    interval_seconds = max(0.05, min(30.0, float(interval_seconds)))
    try:
        previous = file_snapshot(path)
        for _ in range(checks):
            time.sleep(interval_seconds)
            current = file_snapshot(path)
            if current != previous:
                previous = current
                return False, "文件仍在写入"
        with open(path, "rb"):
            pass
    except FileNotFoundError:
        return False, "源文件不存在"
    except PermissionError:
        return False, "文件暂时无法读取"
    except OSError as err:
        return False, f"文件状态检查失败: {err}"
    return True, ""
