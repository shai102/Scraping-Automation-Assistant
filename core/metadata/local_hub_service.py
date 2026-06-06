from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


_FOLDER_TMDB_RE = re.compile(r"(?i)tmdb(?:id)?\s*[-=:_]\s*(\d+)")
_SEASON_DIR_RE = re.compile(r"(?i)^(?:season\s*|s)(\d+)$")


class MetadataHubError(ValueError):
    pass


def inspect_metadata_hub(root_path: str) -> dict:
    root = _validated_root(root_path)
    title_dirs = [item for item in root.iterdir() if item.is_dir()]
    indexed = sum(1 for item in title_dirs if _title_tmdb_id(item))
    return {
        "root": str(root),
        "title_dirs": len(title_dirs),
        "indexed_titles": indexed,
    }


def update_record_from_metadata_hub(record, root_path: str) -> dict:
    if record.status != "success":
        raise MetadataHubError("只能更新已成功的刮削记录")
    if str(record.matched_provider or "tmdb").lower() != "tmdb":
        raise MetadataHubError("Metadata Hub 仅支持按 TMDB ID 更新")

    tmdb_id = str(record.matched_id or "").strip()
    if not tmdb_id or tmdb_id == "None":
        raise MetadataHubError("记录缺少 TMDB ID")

    target_path = Path(str(record.target_path or "")).expanduser()
    if not target_path.is_file():
        raise MetadataHubError(f"目标媒体文件不存在: {target_path}")

    try:
        metadata = json.loads(record.metadata_json or "{}")
    except (TypeError, ValueError):
        metadata = {}

    root = _validated_root(root_path)
    source_dir = _find_title_dir(root, tmdb_id)
    media_type = str(metadata.get("type") or "").strip().lower()
    if media_type == "episode":
        result = _update_tv_record(source_dir, target_path, metadata)
    else:
        result = _update_movie_record(source_dir, target_path)
    return {
        "tmdb_id": tmdb_id,
        "source_dir": str(source_dir),
        "target_path": str(target_path),
        **result,
    }


def _validated_root(root_path: str) -> Path:
    value = str(root_path or "").strip()
    if not value:
        raise MetadataHubError("请先配置 Metadata Hub 本地目录")
    root = Path(value).expanduser()
    if not root.is_dir():
        raise MetadataHubError(f"Metadata Hub 目录不存在: {root}")
    return root.resolve()


def _find_title_dir(root: Path, tmdb_id: str) -> Path:
    for item in sorted(root.iterdir()):
        if item.is_dir() and _title_tmdb_id(item) == tmdb_id:
            return item
    raise MetadataHubError(f"Metadata Hub 中未找到 TMDB ID {tmdb_id}")


def _title_tmdb_id(title_dir: Path) -> str:
    match = _FOLDER_TMDB_RE.search(title_dir.name)
    if match:
        return match.group(1)
    for name in ("tvshow.nfo", "movie.nfo"):
        value = _nfo_tmdb_id(title_dir / name)
        if value:
            return value
    for nfo_path in sorted(title_dir.glob("*.nfo")):
        value = _nfo_tmdb_id(nfo_path)
        if value:
            return value
    return ""


def _nfo_tmdb_id(path: Path) -> str:
    if not path.is_file():
        return ""
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        return ""
    for node in root.findall("uniqueid"):
        if str(node.attrib.get("type") or "").lower() == "tmdb" and node.text:
            return node.text.strip()
    return ""


def _update_movie_record(source_dir: Path, target_path: Path) -> dict:
    source_nfo = source_dir / "movie.nfo"
    if not source_nfo.is_file():
        source_nfo = next(
            (
                item
                for item in sorted(source_dir.glob("*.nfo"))
                if _xml_root_tag(item) == "movie"
            ),
            None,
        )
    if source_nfo is None or not source_nfo.is_file():
        raise MetadataHubError(f"Hub 作品缺少电影 NFO: {source_dir}")

    copied = [
        _copy_required(source_nfo, target_path.with_suffix(".nfo")),
    ]
    copied.extend(
        _copy_optional_assets(
            source_dir,
            target_path.parent,
            (("poster.jpg", "poster.jpg"), ("fanart.jpg", "fanart.jpg")),
        )
    )
    return {"media_type": "movie", "copied": copied}


def _update_tv_record(source_dir: Path, target_path: Path, metadata: dict) -> dict:
    try:
        season = int(metadata.get("s"))
        episode = int(metadata.get("e"))
    except (TypeError, ValueError):
        raise MetadataHubError("记录缺少有效的季号或集号")

    source_season_dir = _find_season_dir(source_dir, season)
    episode_stem = f"S{season:02d}E{episode:02d}"
    source_episode_nfo = source_season_dir / f"{episode_stem}.nfo"
    if not source_episode_nfo.is_file():
        source_episode_nfo = _find_episode_nfo(source_season_dir, season, episode)
    if source_episode_nfo is None:
        raise MetadataHubError(f"Hub 中未找到第 {season} 季第 {episode} 集 NFO")

    target_season_dir, target_root = _target_tv_dirs(target_path)
    copied = [
        _copy_required(source_episode_nfo, target_path.with_suffix(".nfo")),
    ]
    copied.extend(
        _copy_optional_assets(
            source_season_dir,
            target_season_dir,
            ((f"{episode_stem}-thumb.jpg", target_path.stem + "-thumb.jpg"),),
        )
    )

    source_title_nfo = source_dir / "tvshow.nfo"
    if source_title_nfo.is_file():
        copied.append(_copy_required(source_title_nfo, target_root / "tvshow.nfo"))
    copied.extend(
        _copy_optional_assets(
            source_dir,
            target_root,
            (("poster.jpg", "poster.jpg"), ("fanart.jpg", "fanart.jpg")),
        )
    )

    source_season_nfo = source_season_dir / "season.nfo"
    season_fmt = f"{season:02d}"
    if source_season_nfo.is_file():
        copied.append(
            _copy_required(source_season_nfo, target_root / f"season{season_fmt}.nfo")
        )
        if target_season_dir != target_root:
            copied.append(
                _copy_required(source_season_nfo, target_season_dir / "season.nfo")
            )
    copied.extend(
        _copy_optional_assets(
            source_season_dir,
            target_root,
            (("season.jpg", f"season{season_fmt}-poster.jpg"),),
        )
    )
    if target_season_dir != target_root:
        copied.extend(
            _copy_optional_assets(
                source_season_dir,
                target_season_dir,
                (("season.jpg", "folder.jpg"),),
            )
        )
    return {
        "media_type": "episode",
        "season": season,
        "episode": episode,
        "copied": copied,
    }


def _find_season_dir(source_dir: Path, season: int) -> Path:
    direct = source_dir / f"Season {season}"
    if direct.is_dir():
        return direct
    for item in source_dir.iterdir():
        if not item.is_dir():
            continue
        match = _SEASON_DIR_RE.match(item.name.strip())
        if match and int(match.group(1)) == season:
            return item
    raise MetadataHubError(f"Hub 中未找到第 {season} 季目录")


def _find_episode_nfo(source_dir: Path, season: int, episode: int) -> Path | None:
    for path in sorted(source_dir.glob("*.nfo")):
        try:
            root = ET.parse(path).getroot()
            source_season = int((root.findtext("season") or "0").strip())
            source_episode = int((root.findtext("episode") or "0").strip())
        except (ET.ParseError, OSError, TypeError, ValueError):
            continue
        if source_season == season and source_episode == episode:
            return path
    return None


def _target_tv_dirs(target_path: Path) -> tuple[Path, Path]:
    target_dir = target_path.parent
    match = _SEASON_DIR_RE.match(target_dir.name.strip())
    if match:
        return target_dir, target_dir.parent
    return target_dir, target_dir


def _copy_optional_assets(
    source_dir: Path,
    target_dir: Path,
    names: tuple[tuple[str, str], ...],
) -> list[str]:
    copied = []
    for source_name, target_name in names:
        source = source_dir / source_name
        if source.is_file():
            copied.append(_copy_required(source, target_dir / target_name))
    return copied


def _copy_required(source: Path, target: Path) -> str:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
        shutil.copy2(source, temp_path)
        os.replace(temp_path, target)
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)
    return str(target)


def _xml_root_tag(path: Path) -> str:
    try:
        return ET.parse(path).getroot().tag.lower()
    except (ET.ParseError, OSError):
        return ""
