import os
import re

from core.metadata.sidecar_service import _nfo_has_empty_plot, save_image, write_nfo


def write_sidecar_files(ctx, item, target_path):
    target_dir = os.path.dirname(target_path)
    metadata = item.metadata or {}
    media_type = metadata.get("type", "episode")
    is_tv = media_type == "episode"
    is_sub_audio = item.old_name.lower().endswith(ctx.get_sub_audio_exts())

    image_tasks = []

    with ctx.file_write_lock:
        if is_tv:
            if not is_sub_audio:
                episode_nfo = os.path.splitext(target_path)[0] + ".nfo"
                if not os.path.exists(episode_nfo):
                    write_nfo(episode_nfo, metadata, "episodedetails")
                thumb_source = (
                    metadata.get("still")
                    or metadata.get("s_poster")
                    or metadata.get("poster")
                )
                if thumb_source:
                    thumb_path = os.path.splitext(target_path)[0] + "-thumb.jpg"
                    if not os.path.exists(thumb_path):
                        image_tasks.append((thumb_path, thumb_source))

            current_dir = target_dir
            dir_name = os.path.basename(current_dir)
            is_season_folder = bool(
                re.match(r"^(Season\s*\d+|S\d+)$", dir_name, re.I)
            )
            root_dir = (
                os.path.dirname(current_dir)
                if (is_season_folder and os.path.dirname(current_dir))
                else current_dir
            )

            season_num = metadata.get("s", 1)
            try:
                season_fmt = f"{int(season_num):02d}"
            except Exception:
                season_fmt = str(season_num)

            season_nfo_root = os.path.join(root_dir, f"season{season_fmt}.nfo")
            season_poster_root = os.path.join(root_dir, f"season{season_fmt}-poster.jpg")
            if not os.path.exists(season_nfo_root):
                write_nfo(season_nfo_root, metadata, "season")
            if metadata.get("s_poster") and not os.path.exists(season_poster_root):
                image_tasks.append((season_poster_root, metadata["s_poster"]))

            if is_season_folder:
                season_nfo_local = os.path.join(current_dir, "season.nfo")
                folder_jpg_local = os.path.join(current_dir, "folder.jpg")
                if not os.path.exists(season_nfo_local):
                    write_nfo(season_nfo_local, metadata, "season")
                if metadata.get("s_poster") and not os.path.exists(folder_jpg_local):
                    image_tasks.append((folder_jpg_local, metadata["s_poster"]))

            tvshow_nfo = os.path.join(root_dir, "tvshow.nfo")
            poster_path = os.path.join(root_dir, "poster.jpg")
            if not os.path.exists(tvshow_nfo) or _nfo_has_empty_plot(tvshow_nfo):
                write_nfo(tvshow_nfo, metadata, "tvshow")
            if metadata.get("poster") and not os.path.exists(poster_path):
                image_tasks.append((poster_path, metadata["poster"]))
        else:
            if not is_sub_audio:
                movie_nfo = os.path.splitext(target_path)[0] + ".nfo"
                if not os.path.exists(movie_nfo):
                    write_nfo(movie_nfo, metadata, "movie")
            poster_path = os.path.join(target_dir, "poster.jpg")
            if metadata.get("poster") and not os.path.exists(poster_path):
                image_tasks.append((poster_path, metadata["poster"]))
            fanart_path = os.path.join(target_dir, "fanart.jpg")
            if metadata.get("fanart") and not os.path.exists(fanart_path):
                image_tasks.append((fanart_path, metadata["fanart"]))

    for img_path, img_url in image_tasks:
        save_image(img_path, img_url)


def refresh_sidecar_files(ctx, target_path, old_metadata, new_metadata):
    """Re-write NFO files and download missing images after a metadata refresh."""
    target_dir = os.path.dirname(target_path)
    metadata = new_metadata or {}
    media_type = metadata.get("type", "episode")
    is_tv = media_type == "episode"

    old = old_metadata or {}
    image_tasks = []

    def field_improved(key):
        new_val = str(metadata.get(key) or "").strip()
        old_val = str(old.get(key) or "").strip()
        return bool(new_val) and not bool(old_val)

    def list_improved(key):
        return bool(metadata.get(key)) and not bool(old.get(key))

    def rating_improved():
        try:
            new_rating = float(metadata.get("rating") or 0)
            old_rating = float(old.get("rating") or 0)
            return new_rating > 0 and old_rating == 0
        except (TypeError, ValueError):
            return False

    has_improvements = (
        field_improved("overview")
        or field_improved("ep_plot")
        or field_improved("ep_title")
        or field_improved("still")
        or field_improved("poster")
        or field_improved("fanart")
        or field_improved("s_poster")
        or list_improved("actors")
        or list_improved("genres")
        or list_improved("directors")
        or list_improved("studios")
        or rating_improved()
    )

    if not has_improvements:
        return False

    still_upgraded = bool(str(metadata.get("still") or "").strip()) and not bool(
        str(old.get("still") or "").strip()
    )

    with ctx.file_write_lock:
        if is_tv:
            episode_nfo = os.path.splitext(target_path)[0] + ".nfo"
            write_nfo(episode_nfo, metadata, "episodedetails")

            thumb_source = (
                metadata.get("still")
                or metadata.get("s_poster")
                or metadata.get("poster")
            )
            if thumb_source:
                thumb_path = os.path.splitext(target_path)[0] + "-thumb.jpg"
                if still_upgraded and os.path.exists(thumb_path):
                    try:
                        os.remove(thumb_path)
                    except Exception:
                        pass
                if not os.path.exists(thumb_path):
                    image_tasks.append((thumb_path, thumb_source))

            current_dir = target_dir
            dir_name = os.path.basename(current_dir)
            is_season_folder = bool(
                re.match(r"^(Season\s*\d+|S\d+)$", dir_name, re.I)
            )
            root_dir = (
                os.path.dirname(current_dir)
                if (is_season_folder and os.path.dirname(current_dir))
                else current_dir
            )

            season_num = metadata.get("s", 1)
            try:
                season_fmt = f"{int(season_num):02d}"
            except Exception:
                season_fmt = str(season_num)

            season_nfo_root = os.path.join(root_dir, f"season{season_fmt}.nfo")
            write_nfo(season_nfo_root, metadata, "season")

            if metadata.get("s_poster"):
                season_poster_root = os.path.join(
                    root_dir, f"season{season_fmt}-poster.jpg"
                )
                if not os.path.exists(season_poster_root):
                    image_tasks.append((season_poster_root, metadata["s_poster"]))

            if is_season_folder:
                season_nfo_local = os.path.join(current_dir, "season.nfo")
                write_nfo(season_nfo_local, metadata, "season")
                if metadata.get("s_poster"):
                    folder_jpg_local = os.path.join(current_dir, "folder.jpg")
                    if not os.path.exists(folder_jpg_local):
                        image_tasks.append((folder_jpg_local, metadata["s_poster"]))

            tvshow_nfo = os.path.join(root_dir, "tvshow.nfo")
            write_nfo(tvshow_nfo, metadata, "tvshow")

            if metadata.get("poster"):
                poster_path = os.path.join(root_dir, "poster.jpg")
                if not os.path.exists(poster_path):
                    image_tasks.append((poster_path, metadata["poster"]))
        else:
            movie_nfo = os.path.splitext(target_path)[0] + ".nfo"
            write_nfo(movie_nfo, metadata, "movie")

            if metadata.get("poster"):
                poster_path = os.path.join(target_dir, "poster.jpg")
                if not os.path.exists(poster_path):
                    image_tasks.append((poster_path, metadata["poster"]))
            if metadata.get("fanart"):
                fanart_path = os.path.join(target_dir, "fanart.jpg")
                if not os.path.exists(fanart_path):
                    image_tasks.append((fanart_path, metadata["fanart"]))

    for img_path, img_url in image_tasks:
        save_image(img_path, img_url)

    return True
