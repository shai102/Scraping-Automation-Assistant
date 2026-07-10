import logging
import os
import re
import shutil
from typing import Optional

from utils.media_defaults import DEFAULT_SUB_AUDIO_EXTS, DEFAULT_VIDEO_EXTS


logger = logging.getLogger(__name__)

MEDIA_EXTS = tuple(
    ext.strip().lower()
    for ext in (DEFAULT_VIDEO_EXTS + "," + DEFAULT_SUB_AUDIO_EXTS).split(",")
    if ext.strip()
)
SEASON_DIR_RE = re.compile(r"^(?:Season\s*0*(\d+)|S\s*0*(\d+))$", re.I)

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


def output_cleanup_root(folder) -> Optional[str]:
    if not folder:
        return None
    organize_mode = getattr(folder, "organize_mode", "move") or "move"
    if organize_mode == "rename":
        root = getattr(folder, "path", "")
    else:
        root = getattr(folder, "target_root", "") or getattr(folder, "path", "")
    root = str(root or "").strip()
    return os.path.normpath(root) if root else None


def configured_media_exts(watcher) -> tuple[str, ...]:
    worker_ctx = getattr(watcher, "_worker_ctx", None)
    getter = getattr(worker_ctx, "get_media_exts", None)
    if callable(getter):
        try:
            configured = tuple(str(ext).lower() for ext in getter() if str(ext).strip())
            if configured:
                return configured
        except Exception:
            pass
    return MEDIA_EXTS


def directory_has_media(directory: str, media_exts: tuple[str, ...] = MEDIA_EXTS) -> bool:
    if not os.path.isdir(directory):
        return False
    try:
        for _dirpath, _dirnames, filenames in os.walk(directory):
            if any(filename.lower().endswith(media_exts) for filename in filenames):
                return True
    except OSError:
        return True
    return False


def _is_strict_child(path: str, root: str) -> bool:
    try:
        path_abs = os.path.normcase(os.path.abspath(path))
        root_abs = os.path.normcase(os.path.abspath(root))
        return path_abs != root_abs and os.path.commonpath((path_abs, root_abs)) == root_abs
    except (OSError, ValueError):
        return False


def _delete_season_root_sidecars(show_root: str, season_number: int):
    season_names = {
        f"season{season_number}.nfo",
        f"season{season_number:02d}.nfo",
        f"season{season_number}-poster.jpg",
        f"season{season_number:02d}-poster.jpg",
    }
    try:
        filenames = os.listdir(show_root)
    except OSError:
        return
    for filename in filenames:
        if filename.lower() not in season_names:
            continue
        path = os.path.join(show_root, filename)
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)
                logger.debug(f"删除季级伴随文件: {path}")
        except OSError as err:
            logger.warning(f"删除季级伴随文件失败 {path}: {err}")


def cleanup_scraped_output_tree(
    target_path: str,
    cleanup_root: Optional[str],
    media_exts: tuple[str, ...] = MEDIA_EXTS,
) -> bool:
    """Remove empty season/title trees including shared NFO and artwork.

    A title directory is removed only after its media file has been deleted and
    no sibling media remains anywhere below that title.  The configured output
    root is a hard boundary and is never removed.
    """
    if not target_path or not cleanup_root:
        return False

    target_dir = os.path.normpath(os.path.dirname(target_path))
    cleanup_root = os.path.normpath(cleanup_root)
    if not _is_strict_child(target_dir, cleanup_root):
        logger.warning(
            "跳过作品目录清理，目标不在配置根目录内: target=%s | root=%s",
            target_dir,
            cleanup_root,
        )
        return False

    season_match = SEASON_DIR_RE.match(os.path.basename(target_dir))
    title_root = os.path.dirname(target_dir) if season_match else target_dir
    if not _is_strict_child(title_root, cleanup_root):
        return False

    removed = False
    if season_match and not directory_has_media(target_dir, media_exts):
        season_number = int(season_match.group(1) or season_match.group(2))
        try:
            if os.path.isdir(target_dir):
                shutil.rmtree(target_dir)
                logger.info(f"同步删除空季目录及全部元数据: {target_dir}")
                removed = True
        except OSError as err:
            logger.warning(f"删除空季目录失败 {target_dir}: {err}")
            return removed
        _delete_season_root_sidecars(title_root, season_number)

    if os.path.isdir(title_root) and not directory_has_media(title_root, media_exts):
        try:
            shutil.rmtree(title_root)
            logger.info(f"同步删除空作品目录及全部元数据: {title_root}")
            removed = True
        except OSError as err:
            logger.warning(f"删除空作品目录失败 {title_root}: {err}")
    return removed


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
