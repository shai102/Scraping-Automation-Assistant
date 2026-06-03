from core.services.naming_templates import extract_media_suffix, render_filename_template
from utils.value_utils import safe_filename


def build_filename_preview_payload(template: str, is_tv: bool, preserve_media_suffix: bool):
    sample = {
        "title": "正年" if is_tv else "流媒体示例电影",
        "year": "2024",
        "season": "01",
        "episode": "01",
        "ep_name": "无法城市" if is_tv else "",
        "ext": ".strm" if is_tv else ".mkv",
        "source_filename": (
            "正年.S01E01.2160p.TVING.WEB-DL.H265.AAC-ZeroTV.strm"
            if is_tv
            else "流媒体示例电影.2024.2160p.TVING.WEB-DL.H265.AAC-ZeroTV.mkv"
        ),
        "pure_name": (
            "正年.S01E01.2160p.TVING.WEB-DL.H265.AAC-ZeroTV"
            if is_tv
            else "流媒体示例电影.2024.2160p.TVING.WEB-DL.H265.AAC-ZeroTV"
        ),
        "source_provider": "tmdb",
        "media_id": "119495" if is_tv else "939243",
    }

    media_suffix = ""
    if preserve_media_suffix:
        media_suffix = safe_filename(
            extract_media_suffix(sample["source_filename"], sample["pure_name"])
        )

    context = {
        "title": sample["title"],
        "year": sample["year"],
        "season": sample["season"],
        "episode": sample["episode"],
        "ep_name": sample["ep_name"],
        "ext": sample["ext"],
        "media_suffix": media_suffix,
        "parse_source": "preview",
        "source_provider": sample["source_provider"],
        "media_id": sample["media_id"],
        "is_tv": is_tv,
        "original_title": sample["title"],
        "rating": 8.8 if is_tv else 7.9,
        "genres": ["Drama", "Fantasy"] if is_tv else ["Drama", "Mystery"],
        "studios": ["TVING"] if is_tv else ["Netflix"],
        "overview": "Template preview sample.",
        "ep_plot": "Template preview sample episode plot." if is_tv else "",
        "release": "WEB-DL",
    }
    rendered = render_filename_template(template, context, preserve_media_suffix)
    return {
        "preview_name": rendered,
        "media_suffix": media_suffix,
        "sample": {
            "title": sample["title"],
            "year": sample["year"],
            "season": sample["season"],
            "episode": sample["episode"],
            "ep_name": sample["ep_name"],
            "ext": sample["ext"],
            "source_provider": sample["source_provider"],
            "media_id": sample["media_id"],
        },
    }
