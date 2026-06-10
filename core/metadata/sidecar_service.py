import logging
import os
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from xml.dom import minidom

from utils.proxy import session


TIMEOUT_IMAGE_DOWNLOAD = (10, 30)
_image_semaphore = threading.Semaphore(2)


def save_image(path, url_part):
    if not url_part:
        return

    try:
        url = (
            url_part
            if url_part.startswith("http")
            else f"https://image.tmdb.org/t/p/original{url_part}"
        )
        if os.path.exists(path):
            return
        parent_dir = os.path.dirname(path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        with _image_semaphore:
            time.sleep(0.3)
            response = session.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=TIMEOUT_IMAGE_DOWNLOAD,
                stream=True,
            )
            if response.status_code == 200:
                with open(path, "wb") as fh:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            fh.write(chunk)
    except Exception as err:
        logging.error(f"保存图片失败 {path}: {err}")


def write_nfo(path, data, nfo_type="movie"):
    try:
        root = ET.Element(nfo_type)

        if nfo_type == "episodedetails":
            title = data.get("ep_title", "")
            if not title or title == data.get("title"):
                title = f"第 {data.get('e', 1)} 集"

            ET.SubElement(root, "title").text = str(title)
            ET.SubElement(root, "plot").text = str(data.get("ep_plot", ""))
            ET.SubElement(root, "season").text = str(data.get("s", 1))
            ET.SubElement(root, "episode").text = str(data.get("e", 1))
            ET.SubElement(root, "year").text = str(data.get("year") or "")

        elif nfo_type == "season":
            season_num = data.get("s", 1)
            ET.SubElement(root, "title").text = f"第 {season_num} 季"
            ET.SubElement(root, "sorttitle").text = f"第 {season_num} 季"
            ET.SubElement(root, "seasonnumber").text = str(season_num)
            ET.SubElement(root, "plot").text = str(data.get("overview", ""))
            ET.SubElement(root, "year").text = str(data.get("year") or "")

        else:
            ET.SubElement(root, "title").text = str(data.get("title", ""))
            overview = str(data.get("overview", ""))
            ET.SubElement(root, "plot").text = overview
            ET.SubElement(root, "outline").text = overview
            orig_title = str(data.get("original_title", ""))
            if orig_title:
                ET.SubElement(root, "originaltitle").text = orig_title
            ET.SubElement(root, "year").text = str(data.get("year") or "")
            ET.SubElement(root, "dateadded").text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            release = str(data.get("release") or "")
            if release:
                ET.SubElement(root, "premiered").text = release
                ET.SubElement(root, "aired").text = release

            rating = data.get("rating") or 0
            if rating:
                rating_el = ET.SubElement(root, "ratings")
                source_el = ET.SubElement(
                    rating_el, "rating", name="tmdb", max="10", default="true"
                )
                ET.SubElement(source_el, "value").text = f"{float(rating):.1f}"
                votes = data.get("votes") or 0
                if votes:
                    ET.SubElement(source_el, "votes").text = str(int(votes))

            runtime = data.get("runtime")
            if runtime:
                ET.SubElement(root, "runtime").text = str(int(runtime))

            status = str(data.get("status") or "")
            if status:
                ET.SubElement(root, "status").text = status

            for genre in (data.get("genres") or []):
                ET.SubElement(root, "genre").text = str(genre)

            for studio in (data.get("studios") or []):
                ET.SubElement(root, "studio").text = str(studio)

            for director in (data.get("directors") or []):
                ET.SubElement(root, "director").text = str(director)

            for actor in (data.get("actors") or []):
                actor_el = ET.SubElement(root, "actor")
                ET.SubElement(actor_el, "name").text = str(actor.get("name", ""))
                ET.SubElement(actor_el, "role").text = str(actor.get("role", ""))
                if actor.get("thumb"):
                    ET.SubElement(actor_el, "thumb").text = str(actor["thumb"])

        provider = str(data.get("provider") or "tmdb").strip().lower() or "tmdb"
        ET.SubElement(root, "lockdata").text = "false"
        ET.SubElement(root, "uniqueid", type=provider).text = str(data.get("id", ""))

        roots = [root]
        if nfo_type == "episodedetails":
            # 多集合并文件（如 S01E01-E02）：按 Kodi/Emby 规范追加多个 episodedetails 块
            try:
                e_start = int(data.get("e", 1))
                e_end = int(data.get("e_end") or 0)
            except (TypeError, ValueError):
                e_start, e_end = 1, 0
            if e_end > e_start and e_end - e_start <= 30:
                for extra_ep in range(e_start + 1, e_end + 1):
                    extra_root = ET.Element(nfo_type)
                    ET.SubElement(extra_root, "title").text = f"第 {extra_ep} 集"
                    ET.SubElement(extra_root, "plot").text = ""
                    ET.SubElement(extra_root, "season").text = str(data.get("s", 1))
                    ET.SubElement(extra_root, "episode").text = str(extra_ep)
                    ET.SubElement(extra_root, "year").text = str(data.get("year") or "")
                    ET.SubElement(extra_root, "lockdata").text = "false"
                    ET.SubElement(extra_root, "uniqueid", type=provider).text = str(
                        data.get("id", "")
                    )
                    roots.append(extra_root)

        parts = []
        for index, element in enumerate(roots):
            xml_str = ET.tostring(element, encoding="utf-8")
            dom = minidom.parseString(xml_str)
            pretty_xml = dom.toprettyxml(indent="  ")
            lines = [line for line in pretty_xml.split("\n") if line.strip()]
            if index > 0 and lines and lines[0].startswith("<?xml"):
                lines = lines[1:]
            parts.append("\n".join(lines))
        pretty_xml = "\n".join(parts)

        with open(path, "w", encoding="utf-8") as fh:
            fh.write(pretty_xml)
    except Exception as err:
        logging.error(f"写入NFO失败 {path}: {err}")


def _nfo_has_empty_plot(path):
    """Return True if the NFO file exists but has an empty <plot> element."""
    try:
        tree = ET.parse(path)
        plot = tree.find("plot")
        return plot is None or not (plot.text or "").strip()
    except Exception:
        return False
