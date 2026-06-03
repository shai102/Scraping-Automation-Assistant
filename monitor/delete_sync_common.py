import logging
import os
from typing import Optional


logger = logging.getLogger(__name__)

IGNORABLE_FILES = frozenset(
    {
        "desktop.ini",
        "thumbs.db",
        ".ds_store",
        "picasa.ini",
        ".picasa.ini",
        "folder.jpg",
        ".bridgesort",
    }
)


def delete_per_file_sidecars(file_path: str):
    if not file_path:
        return
    base = os.path.splitext(file_path)[0]
    for suffix in (".nfo", "-thumb.jpg", "-poster.jpg", "-fanart.jpg"):
        sidecar = base + suffix
        if os.path.isfile(sidecar):
            try:
                os.remove(sidecar)
                logger.debug(f"删除伴随文件: {sidecar}")
            except Exception as err:
                logger.warning(f"删除伴随文件失败 {sidecar}: {err}")


def dir_real_entries(dir_path: str) -> list[str]:
    try:
        return [name for name in os.listdir(dir_path) if name.lower() not in IGNORABLE_FILES]
    except Exception:
        return ["<error>"]


def remove_empty_dirs(start_dir: str, stop_at: Optional[str] = None):
    current = os.path.normpath(start_dir)
    while True:
        if stop_at and os.path.normcase(current) == os.path.normcase(stop_at):
            break
        parent = os.path.dirname(current)
        if parent == current:
            break
        try:
            if not os.path.isdir(current):
                break
            real_entries = dir_real_entries(current)
            if real_entries:
                break
            for name in os.listdir(current):
                try:
                    os.remove(os.path.join(current, name))
                except Exception:
                    pass
            os.rmdir(current)
            logger.debug(f"Removed empty dir: {current}")
        except Exception as err:
            logger.warning(f"Could not remove dir {current}: {err}")
            break
        current = parent
