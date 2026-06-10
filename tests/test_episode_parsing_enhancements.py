"""识别增强回归测试：集数范围、中文数字、guessit 缓存、小数集检测。"""

import os
import sys
import tempfile
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.episode_parsing import (
    cached_guessit,
    cn_numeral_to_int,
    extract_episode_number,
    extract_episode_range,
    is_decimal_episode,
)


def test_cn_numeral_to_int():
    assert cn_numeral_to_int("十二") == 12
    assert cn_numeral_to_int("二十五") == 25
    assert cn_numeral_to_int("一百零三") == 103
    assert cn_numeral_to_int("二〇五") == 205
    assert cn_numeral_to_int("两百") == 200
    assert cn_numeral_to_int("") is None
    assert cn_numeral_to_int("第") is None


def test_chinese_numeral_episode():
    assert extract_episode_number("快乐再出发 第十二集 4K") == 12
    assert extract_episode_number("舌尖上的中国 第二十五集") == 25
    assert extract_episode_number("纪录片 第一百零三集") == 103
    # 阿拉伯数字优先级不受影响
    assert extract_episode_number("某剧 第3集") == 3


def test_episode_range_standard():
    assert extract_episode_range("Show.S01E01-E02.1080p") == (1, 2)
    assert extract_episode_range("Show S01E03E04") == (3, 4)
    assert extract_episode_range("Show.S01E05-06.WEB-DL") == (5, 6)
    assert extract_episode_range("Title EP01-02") == (1, 2)
    assert extract_episode_range("[Sub] Title 第01-02话") == (1, 2)
    assert extract_episode_range("[Sub] Title [03-04]") == (3, 4)


def test_episode_range_rejects_noise():
    # 年份范围不是集数范围
    assert extract_episode_range("Title [2023-2024]") is None
    # 分辨率不构成范围
    assert extract_episode_range("Show.S01E08.1080p") is None
    # 单集不返回范围
    assert extract_episode_range("Show.S01E05") is None
    # 跨度过大拒绝
    assert extract_episode_range("Show.S01E01-E99") is None
    # 倒序拒绝
    assert extract_episode_range("Title [05-03]") is None


def test_episode_range_from_guessit_list():
    assert extract_episode_range("whatever", {"episode": [7, 8]}) == (7, 8)
    # 不连续列表拒绝
    assert extract_episode_range("whatever", {"episode": [1, 3]}) is None
    assert extract_episode_range("whatever", {"episode": [5]}) is None


def test_episode_number_returns_range_start():
    assert extract_episode_number("[Sub] Title 第01-02话") == 1
    assert extract_episode_number("Show.S01E01-E02.1080p") == 1


def test_is_decimal_episode_with_guess_data():
    # 传入已有 guessit 结果时不再二次解析
    assert is_decimal_episode("anything", {"episode": 7.5}) is True
    assert is_decimal_episode("Show.S01E07", {"episode": 7}) is False
    # 正则兜底仍工作
    assert is_decimal_episode("Show S01E07.5", {"episode": None}) is True


def test_cached_guessit_consistency():
    first = cached_guessit("Show.S01E05.1080p.mkv")
    second = cached_guessit("Show.S01E05.1080p.mkv")
    assert first == second
    assert first.get("episode") == 5
    # 返回副本，修改不污染缓存
    first["episode"] = 999
    assert cached_guessit("Show.S01E05.1080p.mkv").get("episode") == 5


def test_decimal_re_single_source():
    from utils.episode_parsing import DECIMAL_EPISODE_RE as src
    from core.recognition.preview_parse import DECIMAL_EPISODE_RE as preview

    assert src is preview, "预览管线与解析层必须共享同一份小数集正则"


def test_write_nfo_multi_episode_blocks():
    from core.metadata.sidecar_service import write_nfo

    data = {
        "id": "12345",
        "provider": "tmdb",
        "title": "测试剧集",
        "ep_title": "第 1-2 集",
        "ep_plot": "合集剧情",
        "s": 1,
        "e": 1,
        "e_end": 2,
        "year": 2026,
    }
    with tempfile.TemporaryDirectory() as tmp:
        nfo_path = os.path.join(tmp, "test.nfo")
        write_nfo(nfo_path, data, "episodedetails")
        content = open(nfo_path, encoding="utf-8").read()

    assert content.count("<episodedetails>") == 2
    # 逐块可解析且集数正确
    blocks = content.split("</episodedetails>")
    first = ET.fromstring(blocks[0].split("?>")[-1] + "</episodedetails>")
    second = ET.fromstring(blocks[1] + "</episodedetails>")
    assert first.find("episode").text == "1"
    assert second.find("episode").text == "2"
    assert second.find("uniqueid").text == "12345"


def test_write_nfo_single_episode_unchanged():
    from core.metadata.sidecar_service import write_nfo

    data = {"id": "99", "provider": "tmdb", "ep_title": "正常单集", "s": 1, "e": 3, "e_end": None, "year": 2026}
    with tempfile.TemporaryDirectory() as tmp:
        nfo_path = os.path.join(tmp, "single.nfo")
        write_nfo(nfo_path, data, "episodedetails")
        content = open(nfo_path, encoding="utf-8").read()

    assert content.count("<episodedetails>") == 1
    root = ET.fromstring(content.split("?>")[-1])
    assert root.find("episode").text == "3"


def test_metadata_title_keeps_original_chars():
    """元数据标题不应被 safe_filename 清洗（冒号等保留）。"""
    import inspect
    from core.recognition import preview_population, preview_ui
    from monitor import file_processor_fastpath

    pop_src = inspect.getsource(preview_population.populate_preview_item)
    assert '"title": match_state["std_title"]' in pop_src

    ui_src = inspect.getsource(preview_ui.bg_update_single_ui)
    assert '"title": title' in ui_src

    fast_src = inspect.getsource(file_processor_fastpath.try_nfo_fast_path)
    assert '"title": series_title' in fast_src


def test_media_type_cn_episode_overrides_guessit_movie():
    """guessit 把「第十二集」误判为电影时应被明确剧集标记纠正。"""
    from core.services.worker_context_media import _EXPLICIT_EP_MARKER_RE

    assert _EXPLICIT_EP_MARKER_RE.search("舌尖上的中国 第十二集 2160p")
    assert _EXPLICIT_EP_MARKER_RE.search("某剧 第3集")
    assert _EXPLICIT_EP_MARKER_RE.search("Show S01E05")
    assert not _EXPLICIT_EP_MARKER_RE.search("Inception 2010 1080p")
    assert not _EXPLICIT_EP_MARKER_RE.search("第九区 District 9")


def test_derive_title_strips_cn_episode():
    from utils.title_cleanup import derive_title_from_filename

    assert derive_title_from_filename("舌尖上的中国 第十二集 2160p") == "舌尖上的中国"
    assert derive_title_from_filename("快乐再出发 第3集") == "快乐再出发"
