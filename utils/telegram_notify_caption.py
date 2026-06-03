_MEDIA_EXTS = {
    ".strm",
    ".mp4",
    ".mkv",
    ".ts",
    ".iso",
    ".rmvb",
    ".avi",
    ".mov",
    ".mpeg",
    ".mpg",
    ".wmv",
    ".3gp",
    ".asf",
    ".m4v",
    ".flv",
    ".m2ts",
    ".tp",
    ".f4v",
}


def build_caption(
    folder_name: str,
    items: list,
    total_ep: int,
    file_count: int = 0,
    existing_count: int = 0,
) -> str:
    meta = items[0].metadata or {}
    title = meta.get("title", "未知")
    year = meta.get("year", "")
    media_type = meta.get("type", "")
    season = meta.get("s")
    tmdb_id = meta.get("id", "")
    genres = meta.get("genres", "")
    if isinstance(genres, list):
        genres = "、".join(genres)

    if not file_count:
        file_count = len(items)

    title_line = f"{title}"
    if year:
        title_line += f"-({year})"

    if media_type == "episode" and season is not None:
        s_str = f"S{int(season):02d}"
        episodes = sorted(
            set(
                int(it.metadata.get("e", 0))
                for it in items
                if it.metadata and it.metadata.get("e")
            )
        )
        if episodes:
            if len(episodes) == 1:
                ep_range = f"E{episodes[0]:02d}"
            else:
                ep_range = f"E{episodes[0]:02d}-E{episodes[-1]:02d}"
            title_line += f" {s_str} {ep_range}"
        else:
            title_line += f" {s_str}"

    lines = [f"🖥 新片入库：  {title_line}"]
    if genres:
        lines.append(f"📁 分类：{genres}")
    if folder_name:
        lines.append(f"📂 来源：{folder_name}")
    lines.append(f"📄 本次入库：{file_count}集")

    if media_type == "episode" and season is not None:
        s_str = f"S{int(season):02d}"
        if total_ep > 0:
            missing = total_ep - existing_count
            if missing > 0:
                lines.append(
                    f"📅 本季：{s_str} 已有{existing_count}集，缺{missing}集（共{total_ep}集）"
                )
            else:
                lines.append(f"📅 本季：{s_str} 已有{existing_count}集，共{total_ep}集")
        elif existing_count > 0:
            lines.append(f"📅 本季：{s_str} 已有{existing_count}集")
        else:
            lines.append(f"📅 本季：{s_str}")

    if tmdb_id and str(tmdb_id) != "None":
        lines.append(f"🎬 影号：{tmdb_id}")

    lines.append("")
    lines.append('✨「又有新片可以看了，快来探索吧。」')
    return "\n".join(lines)


def get_poster_url(items: list) -> str:
    for item in items:
        meta = item.metadata or {}
        for key in ("s_poster", "poster"):
            val = meta.get(key, "")
            if val:
                if val.startswith("http"):
                    return val
                return f"https://image.tmdb.org/t/p/w500{val}"
    return ""
