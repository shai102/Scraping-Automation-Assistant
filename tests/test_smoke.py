import logging
import sys
import threading
import time
import unittest
from unittest.mock import patch

from ai.ollama_ai import _extract_siliconflow_content, is_ai_rate_limited_error
from api.routes.recognition_test import _mode_config
from api.routes.settings import _extract_local_model_names
from core.services.worker_context import WorkerContext
from main import _is_ignorable_connection_reset
from monitor.watcher import FolderWatcher
from core.services.matcher_service import (
    _parse_candidate_pick_response,
    auto_pick_candidate_by_score,
    extract_ollama_model_names,
    get_online_embedding,
    pick_candidate_with_openai_compatible,
)
from core.services.naming_service import (
    can_reuse_dir_ai,
    extract_explicit_season,
    extract_media_suffix,
    pick_season,
    render_filename_template,
)
from core.workers.task_runner import (
    SPECIAL_TAG_RE,
    _can_reuse_same_folder_season_cache,
    _dir_cache_key,
    _fetch_ai_parse,
    _guessit_needs_assist,
    _is_meaningful_title,
)
from utils.helpers import (
    bypass_api_cache,
    build_db_query_plan,
    build_query_titles,
    cached_request,
    derive_title_from_filename,
    extract_episode_number,
    format_error_message,
    normalize_proxy_url,
    normalize_parse_source,
    parse_error_message,
    proxy_bypass_url,
    safe_filename,
)


class SmokeTests(unittest.TestCase):
    def test_safe_filename_replaces_illegal_chars(self):
        original = 'a<b>:"c/\\d|?*.'
        self.assertEqual(safe_filename(original), "a_b___c__d___")

    def test_proxy_url_shorthand_is_normalized(self):
        self.assertEqual(normalize_proxy_url("127.0.0.1:7890"), "http://127.0.0.1:7890")
        self.assertEqual(normalize_proxy_url("https://proxy.example.com/"), "https://proxy.example.com")

    def test_proxy_bypass_matches_local_defaults(self):
        self.assertTrue(proxy_bypass_url("http://localhost:11434/api/tags"))
        self.assertTrue(proxy_bypass_url("http://127.0.0.1:8090/api"))
        self.assertTrue(proxy_bypass_url("http://host.docker.internal:7890"))
        self.assertTrue(proxy_bypass_url("http://192.168.100.195:8090"))
        self.assertFalse(proxy_bypass_url("https://api.themoviedb.org/3/configuration"))

    def test_extract_media_suffix_from_standard_episode_name(self):
        name = "Signal.S01E02.2160p.TVING.WEB-DL.H.265.AAC-ColorTV.strm"
        self.assertEqual(
            extract_media_suffix(name),
            "2160p.TVING.WEB-DL.H.265.AAC-ColorTV",
        )

    def test_extract_episode_prefers_explicit_bracket_number(self):
        name = (
            "[Nekomoe kissaten&VCB-Studio] Kage no Jitsuryokusha ni Naritakute! "
            "S2 [10][Ma10p_1080p][x265_flac].strm"
        )
        self.assertEqual(extract_episode_number(name, {"episode": 1}), 10)

    def test_extract_episode_ignores_bracketed_year_noise(self):
        name = "[Group] Some Anime [2024][1080p][x265].mkv"
        self.assertIsNone(extract_episode_number(name))

    def test_render_filename_template_appends_media_suffix(self):
        rendered = render_filename_template(
            "{title} - S{s:02d}E{e:02d} - {ep_name}{ext}",
            {
                "title": "信号",
                "season": "01",
                "episode": "02",
                "ep_name": "回响",
                "ext": ".strm",
                "media_suffix": "2160p.TVING.WEB-DL.H.265.AAC-ColorTV",
            },
            preserve_media_suffix=True,
        )
        self.assertEqual(
            rendered,
            "信号 - S01E02 - 回响 - 2160p.TVING.WEB-DL.H.265.AAC-ColorTV.strm",
        )

    def test_worker_context_render_media_filename_respects_setting(self):
        ctx = WorkerContext({"preserve_media_suffix": True})
        rendered, suffix = ctx._render_media_filename(
            "{title} - S{s:02d}E{e:02d} - {ep_name}{ext}",
            title="信号",
            season="01",
            episode="02",
            ep_name="回响",
            ext=".strm",
            source_filename="Signal.S01E02.2160p.TVING.WEB-DL.H.265.AAC-ColorTV.strm",
            pure_name="Signal.S01E02.2160p.TVING.WEB-DL.H.265.AAC-ColorTV",
        )
        self.assertEqual(suffix, "2160p.TVING.WEB-DL.H.265.AAC-ColorTV")
        self.assertEqual(
            rendered,
            "信号 - S01E02 - 回响 - 2160p.TVING.WEB-DL.H.265.AAC-ColorTV.strm",
        )

    def test_extract_siliconflow_content_success(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "content": '{"title":"Test","year":2024,"season":1,"episode":1}'
                    }
                }
            ]
        }
        self.assertEqual(
            _extract_siliconflow_content(payload),
            '{"title":"Test","year":2024,"season":1,"episode":1}',
        )

    def test_extract_siliconflow_content_rejects_invalid_shape(self):
        with self.assertRaises(ValueError):
            _extract_siliconflow_content({"choices": []})

    def test_windows_asyncio_connection_reset_log_is_ignored(self):
        try:
            raise ConnectionResetError(10054, "远程主机强迫关闭了一个现有的连接。")
        except ConnectionResetError:
            record = logging.LogRecord(
                "asyncio",
                logging.ERROR,
                __file__,
                1,
                "Exception in callback _ProactorBasePipeTransport._call_connection_lost()",
                (),
                sys.exc_info(),
            )

        self.assertTrue(_is_ignorable_connection_reset(record))

    def test_extract_siliconflow_content_accepts_content_parts(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "content": [
                            {"type": "text", "text": '{"title":"Frieren","episode":1}'}
                        ]
                    }
                }
            ]
        }
        self.assertEqual(
            _extract_siliconflow_content(payload),
            '{"title":"Frieren","episode":1}',
        )

    def test_extract_siliconflow_content_falls_back_to_reasoning(self):
        payload = {
            "choices": [
                {
                    "message": {
                        "content": [],
                        "reasoning": '{"title":"Frieren","episode":1}',
                    }
                }
            ]
        }
        self.assertEqual(
            _extract_siliconflow_content(payload),
            '{"title":"Frieren","episode":1}',
        )

    def test_extract_ollama_model_names_success(self):
        payload = {
            "models": [
                {"name": "qwen2.5:14b"},
                {"name": "nomic-embed-text:latest"},
                {"name": "qwen2.5:14b"},
            ]
        }
        self.assertEqual(
            extract_ollama_model_names(payload),
            ["qwen2.5:14b", "nomic-embed-text:latest"],
        )

    def test_extract_local_model_names_accepts_openai_compatible_shape(self):
        payload = {
            "data": [
                {"id": "qwen3:8b"},
                {"id": "nomic-embed-text"},
            ]
        }
        self.assertEqual(
            _extract_local_model_names(payload),
            ["qwen3:8b", "nomic-embed-text"],
        )

    def test_get_online_embedding_uses_openai_compatible_endpoint(self):
        class FakeResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return {"data": [{"embedding": [0.1, 0.2, 0.3]}]}

        cache = {}
        lock = threading.Lock()
        with patch("core.services.matcher_service.request_post", return_value=FakeResponse()) as post:
            emb = get_online_embedding(
                "https://api.example.com/v1",
                "sk-test",
                "provider/embed-model",
                "hello",
                cache,
                lock,
            )

        self.assertEqual(emb, [0.1, 0.2, 0.3])
        args, kwargs = post.call_args
        self.assertEqual(args[0], "https://api.example.com/v1/embeddings")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer sk-test")
        self.assertEqual(kwargs["json"]["model"], "provider/embed-model")
        self.assertEqual(kwargs["json"]["input"], "hello")

    def test_worker_context_matches_desktop_embedding_rank_policy(self):
        ctx = WorkerContext(config={
            "use_embedding_rank": True,
            "embedding_source": "online",
            "sf_api_url": "https://api.example.com/v1",
            "sf_api_key": "sk-test",
            "online_embedding_model": "provider/embed-model",
        })
        self.assertFalse(ctx._can_use_embedding_rank())

        local_ctx = WorkerContext(config={
            "use_embedding_rank": True,
            "prefer_ollama": True,
            "ollama_url": "http://localhost:11434",
            "embedding_model": "nomic-embed-text",
        })
        self.assertTrue(local_ctx._can_use_embedding_rank())

    def test_recognition_online_ai_keeps_local_embedding(self):
        cfg = _mode_config(
            {
                "ollama_model": "qwen-local",
                "embedding_model": "nomic-embed-text",
                "embedding_source": "online",
                "online_embedding_model": "provider/embed-model",
                "prefer_ollama": True,
            },
            "online_ai",
        )
        self.assertEqual(cfg["ai_mode"], "force")
        self.assertFalse(cfg["prefer_ollama"])
        self.assertEqual(cfg["ollama_model"], "")
        self.assertEqual(cfg["embedding_source"], "local")
        self.assertEqual(cfg["embedding_model"], "nomic-embed-text")
        self.assertEqual(cfg["online_embedding_model"], "")

    def test_prefer_ollama_parse_does_not_fallback_to_online(self):
        ctx = WorkerContext(config={
            "prefer_ollama": True,
            "ollama_url": "http://localhost:11434",
            "ollama_model": "qwen-local",
            "sf_api_key": "sk-test",
            "sf_api_url": "https://api.example.com/v1",
            "sf_model": "provider/chat-model",
        })
        with (
            patch.object(
                ctx,
                "_parse_with_ollama",
                return_value=(None, "local parse failed"),
            ),
            patch(
                "core.workers.task_runner.fetch_siliconflow_info",
                side_effect=AssertionError("online parse should not be used"),
            ) as online_parse,
        ):
            ai_data, ai_msg = _fetch_ai_parse(ctx, "Ambiguous.Title.S01E01.mkv")

        self.assertIsNone(ai_data)
        self.assertEqual(ai_msg, "local parse failed")
        online_parse.assert_not_called()

    def test_pick_candidate_with_openai_compatible_selects_candidate(self):
        class FakeResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return {
                    "choices": [
                        {"message": {"content": '{"pick": 2, "reason": "标题和年份匹配"}'}}
                    ]
                }

        candidates = [
            {"id": "1", "title": "Wrong", "release": "2024-01-01"},
            {"id": "2", "title": "Right", "release": "2024-01-01"},
        ]
        item = {"old_name": "Right.S01E01.2024.mkv"}
        with patch(
            "core.services.matcher_service._post_openai_compatible",
            return_value=FakeResponse(),
        ) as post:
            chosen, reason = pick_candidate_with_openai_compatible(
                "https://openrouter.ai/api/v1",
                "sk-test",
                "provider/chat-model",
                item,
                "Right",
                2024,
                True,
                "TMDb",
                candidates,
            )

        self.assertEqual(chosen["id"], "2")
        self.assertIn("匹配", reason)
        args, kwargs = post.call_args
        self.assertEqual(args[0], "https://openrouter.ai/api/v1/chat/completions")
        self.assertEqual(args[1]["model"], "provider/chat-model")

    def test_online_model_judges_candidates_before_local_ollama(self):
        ctx = WorkerContext(config={
            "prefer_ollama": False,
            "sf_api_url": "https://api.example.com/v1",
            "sf_api_key": "sk-test",
            "sf_model": "provider/chat-model",
            "ollama_url": "http://localhost:11434",
            "ollama_model": "qwen-local",
            "use_embedding_rank": False,
        })
        item = {"old_name": "Ambiguous.Title.S01E01.mkv"}
        candidates = [
            {
                "id": "1",
                "title": "Wrong Series",
                "alt_title": "",
                "release": "2024-01-01",
                "meta": {"search_query": "Ambiguous Title", "search_rank": 1},
            },
            {
                "id": "2",
                "title": "Right Series",
                "alt_title": "",
                "release": "2024-01-01",
                "meta": {"search_query": "Ambiguous Title", "search_rank": 2},
            },
        ]
        with (
            patch.object(
                ctx,
                "_pick_candidate_with_online_model",
                return_value=(candidates[1], "online chose"),
            ) as online_pick,
            patch.object(
                ctx,
                "_pick_candidate_with_ollama",
                side_effect=AssertionError("local Ollama should not judge first"),
            ) as ollama_pick,
        ):
            _title, matched_id, message, _meta = ctx._select_best_db_match(
                item,
                "Ambiguous Title",
                None,
                True,
                "TMDb",
                candidates,
            )

        self.assertEqual(matched_id, "2")
        self.assertIn("在线模型判定", message)
        online_pick.assert_called_once()
        ollama_pick.assert_not_called()

    def test_online_model_uncertain_does_not_fall_back_to_tmdb_first(self):
        ctx = WorkerContext(config={
            "prefer_ollama": False,
            "sf_api_url": "https://api.example.com/v1",
            "sf_api_key": "sk-test",
            "sf_model": "provider/chat-model",
            "use_embedding_rank": False,
        })
        item = {"old_name": "Ambiguous.Title.S01E01.mkv"}
        candidates = [
            {
                "id": "1",
                "title": "Wrong Series",
                "alt_title": "",
                "release": "2024-01-01",
                "meta": {"search_query": "Ambiguous Title", "search_rank": 1},
            },
            {
                "id": "2",
                "title": "Another Series",
                "alt_title": "",
                "release": "2024-01-01",
                "meta": {"search_query": "Ambiguous Title", "search_rank": 2},
            }
        ]
        with patch.object(
            ctx,
            "_pick_candidate_with_online_model",
            return_value=(None, "uncertain"),
        ):
            _title, matched_id, _message, _meta = ctx._select_best_db_match(
                item,
                "Ambiguous Title",
                None,
                True,
                "TMDb",
                candidates,
            )

        self.assertEqual(matched_id, "None")

    def test_online_model_remains_final_judge_after_embedding_rerank(self):
        ctx = WorkerContext(config={
            "prefer_ollama": False,
            "sf_api_url": "https://api.example.com/v1",
            "sf_api_key": "sk-test",
            "sf_model": "provider/chat-model",
            "use_embedding_rank": True,
        })
        item = {"old_name": "Ambiguous.Title.S01E01.mkv"}
        candidates = [
            {"id": "1", "title": "Wrong Series", "alt_title": "", "release": "2024-01-01"},
            {"id": "2", "title": "Right Series", "alt_title": "", "release": "2024-01-01"},
        ]
        ranked = [candidates[1], candidates[0]]
        with (
            patch.object(
                ctx,
                "_rerank_candidates_with_embedding",
                return_value=(ranked, candidates[1], "embedding top=0.91"),
            ) as rerank,
            patch.object(
                ctx,
                "_pick_candidate_with_online_model",
                return_value=(ranked[0], "online confirmed"),
            ) as online_pick,
        ):
            _title, matched_id, message, _meta = ctx._select_best_db_match(
                item,
                "Ambiguous Title",
                None,
                True,
                "TMDb",
                candidates,
            )

        self.assertEqual(matched_id, "2")
        self.assertIn("在线模型判定", message)
        self.assertIn("embedding top=0.91", message)
        rerank.assert_called_once()
        online_pick.assert_called_once()
        self.assertEqual(online_pick.call_args[0][5][0]["id"], "2")

    def test_prefer_ollama_final_judge_does_not_fallback_to_online(self):
        ctx = WorkerContext(config={
            "prefer_ollama": True,
            "sf_api_url": "https://api.example.com/v1",
            "sf_api_key": "sk-test",
            "sf_model": "provider/chat-model",
            "ollama_url": "http://localhost:11434",
            "ollama_model": "qwen-local",
            "use_embedding_rank": False,
        })
        item = {"old_name": "Ambiguous.Title.S01E01.mkv"}
        candidates = [
            {"id": "1", "title": "Wrong Series", "alt_title": "", "release": "2024-01-01"},
            {"id": "2", "title": "Right Series", "alt_title": "", "release": "2024-01-01"},
        ]
        with (
            patch.object(
                ctx,
                "_pick_candidate_with_ollama",
                return_value=(None, "local uncertain"),
            ) as ollama_pick,
            patch.object(
                ctx,
                "_pick_candidate_with_online_model",
                side_effect=AssertionError("online final judge should not be used"),
            ) as online_pick,
        ):
            _title, matched_id, _message, _meta = ctx._select_best_db_match(
                item,
                "Ambiguous Title",
                None,
                True,
                "TMDb",
                candidates,
            )

        self.assertEqual(matched_id, "None")
        ollama_pick.assert_called_once()
        online_pick.assert_not_called()

    def test_prefer_ollama_remains_final_judge_after_embedding_rerank(self):
        ctx = WorkerContext(config={
            "prefer_ollama": True,
            "ollama_url": "http://localhost:11434",
            "ollama_model": "qwen-local",
            "use_embedding_rank": True,
        })
        item = {"old_name": "Ambiguous.Title.S01E01.mkv"}
        candidates = [
            {"id": "1", "title": "Wrong Series", "alt_title": "", "release": "2024-01-01"},
            {"id": "2", "title": "Right Series", "alt_title": "", "release": "2024-01-01"},
        ]
        ranked = [candidates[1], candidates[0]]
        with (
            patch.object(
                ctx,
                "_rerank_candidates_with_embedding",
                return_value=(ranked, candidates[1], "embedding top=0.91"),
            ) as rerank,
            patch.object(
                ctx,
                "_pick_candidate_with_ollama",
                return_value=(ranked[0], "local confirmed"),
            ) as ollama_pick,
        ):
            _title, matched_id, message, _meta = ctx._select_best_db_match(
                item,
                "Ambiguous Title",
                None,
                True,
                "TMDb",
                candidates,
            )

        self.assertEqual(matched_id, "2")
        self.assertIn("Ollama判定", message)
        self.assertIn("embedding top=0.91", message)
        rerank.assert_called_once()
        ollama_pick.assert_called_once()
        self.assertEqual(ollama_pick.call_args[0][5][0]["id"], "2")

    def test_extract_ollama_model_names_rejects_invalid_shape(self):
        with self.assertRaises(ValueError):
            extract_ollama_model_names({"models": "bad"})

    def test_error_message_format_and_parse(self):
        msg = format_error_message("timeout", "请求超时")
        self.assertEqual(msg, "TIMEOUT:请求超时")
        self.assertEqual(parse_error_message(msg), ("TIMEOUT", "请求超时"))

    def test_error_message_parse_legacy_text(self):
        self.assertEqual(parse_error_message("未配置TMDb Key")[0], "CONFIG")

    def test_cached_request_bypass_calls_api_without_cache_write(self):
        calls = []

        def fake_api():
            calls.append("called")
            return {"fresh": True}

        with bypass_api_cache(True):
            result = cached_request(fake_api, "unit_test_bypass_key")

        self.assertEqual(result, {"fresh": True})
        self.assertEqual(calls, ["called"])

    def test_build_query_titles_filters_generic_season_title(self):
        item = {
            "old_name": "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC.DDP-Xiaomi.strm",
            "dir": r"D:\Media\Season 1",
        }
        g = {"title": "Extracurricular"}
        titles = build_query_titles(item, "Season 1", None, g)
        self.assertIn("Extracurricular", titles)
        self.assertNotIn("Season 1", titles)

    def test_build_query_titles_keeps_real_title(self):
        item = {
            "old_name": "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC.DDP-Xiaomi.strm",
            "dir": r"D:\Media\Season 1",
        }
        g = {"title": "Extracurricular"}
        titles = build_query_titles(item, "Extracurricular", None, g)
        self.assertIn("Extracurricular", titles)

    def test_build_db_query_plan_prefers_ai_only_when_guessit_title_missing(self):
        item = {
            "old_name": "[Lilith-Raws][Sousou no Frieren] - 01 [Baha][WEB-DL][1080p][AVC AAC][CHT].mkv",
            "dir": r"Y:\test\AI_Assist_01_Sousou_no_Frieren",
        }
        ai_data = {"title": "Sousou no Frieren"}
        g = {"title": "Baha"}
        self.assertEqual(
            build_db_query_plan(item, "Baha", ai_data, g),
            [["Sousou no Frieren"]],
        )

    def test_derive_title_from_group_release_brackets(self):
        pure = "[Lilith-Raws][Sousou no Frieren] - 01 [Baha][WEB-DL][1080p][AVC AAC][CHT]"
        self.assertEqual(derive_title_from_filename(pure), "Sousou no Frieren")

    def test_derive_title_skips_multi_word_release_group(self):
        pure = "[Nekomoe kissaten][Make Heroine ga Oosugiru!] - 01 [WebRip 1080p HEVC AAC][CHT]"
        self.assertEqual(derive_title_from_filename(pure), "Make Heroine ga Oosugiru")

    def test_build_db_query_plan_ignores_release_group_title(self):
        item = {
            "old_name": "[Nekomoe kissaten][Make Heroine ga Oosugiru!] - 01 [WebRip 1080p HEVC AAC][CHT].mkv",
            "dir": "",
        }
        g = {"title": "Nekomoe kissaten"}
        self.assertEqual(
            build_db_query_plan(item, "Nekomoe kissaten", None, g),
            [["Make Heroine ga Oosugiru"]],
        )

    def test_derive_title_from_single_release_group_prefix(self):
        pure = "[Sakurato] Chainsaw Man [S01E01][HEVC-10bit 1080P@60FPS AAC][CHS&CHT]"
        self.assertEqual(derive_title_from_filename(pure), "Chainsaw Man")

    def test_build_db_query_plan_uses_single_release_group_title_only(self):
        item = {
            "old_name": "[Sakurato] Chainsaw Man [S01E01][HEVC-10bit 1080P@60FPS AAC][CHS&CHT].strm",
            "dir": "",
        }
        g = {"title": "Chainsaw Man", "season": 1, "episode": 1}
        self.assertEqual(
            build_db_query_plan(item, "Chainsaw Man", None, g),
            [["Chainsaw Man"]],
        )

    def test_tmdb_single_candidate_uses_desktop_auto_score_fallback(self):
        ctx = WorkerContext(config={})
        item = {
            "old_name": "[Nekomoe kissaten][Make Heroine ga Oosugiru!] - 01 [WebRip 1080p HEVC AAC][CHT].mkv"
        }
        candidates = [
            {
                "id": "259140",
                "title": "Wrong Result",
                "alt_title": "",
                "release": "2024-10-06",
                "meta": {"search_query": "Nekomoe kissaten", "search_rank": 1},
            }
        ]
        _title, tmdb_id, _msg, _meta = ctx._select_best_db_match(
            item,
            "Nekomoe kissaten",
            None,
            True,
            "TMDb",
            candidates,
        )
        self.assertEqual(tmdb_id, "259140")

    def test_tmdb_extra_candidate_filtered_for_regular_episode(self):
        ctx = WorkerContext(config={})
        item = {
            "old_name": "[Sakurato] Chainsaw Man [S01E01][HEVC-10bit 1080P@60FPS AAC][CHS&CHT].strm"
        }
        candidates = [
            {
                "id": "299555",
                "title": "链锯人 总集篇",
                "alt_title": "チェンソーマン 総集篇",
                "release": "2025-09-05",
                "meta": {
                    "original_title": "チェンソーマン 総集篇",
                    "search_query": "Chainsaw Man",
                    "search_rank": 1,
                },
            },
            {
                "id": "114410",
                "title": "链锯人",
                "alt_title": "チェンソーマン",
                "release": "2022-10-12",
                "meta": {
                    "original_title": "チェンソーマン",
                    "search_query": "Chainsaw Man",
                    "search_rank": 2,
                },
            },
        ]
        _title, tmdb_id, _msg, _meta = ctx._select_best_db_match(
            item,
            "Chainsaw Man",
            None,
            True,
            "TMDb",
            candidates,
        )
        self.assertEqual(tmdb_id, "114410")

    def test_tmdb_only_extra_candidate_requires_manual_for_regular_episode(self):
        ctx = WorkerContext(config={})
        item = {
            "old_name": "[Sakurato] Chainsaw Man [S01E01][HEVC-10bit 1080P@60FPS AAC][CHS&CHT].strm"
        }
        candidates = [
            {
                "id": "299555",
                "title": "链锯人 总集篇",
                "alt_title": "チェンソーマン 総集篇",
                "release": "2025-09-05",
                "meta": {"original_title": "チェンソーマン 総集篇"},
            }
        ]
        _title, tmdb_id, _msg, _meta = ctx._select_best_db_match(
            item,
            "Chainsaw Man",
            None,
            True,
            "TMDb",
            candidates,
        )
        self.assertEqual(tmdb_id, "None")

    def test_tmdb_unrequested_variant_candidate_is_filtered(self):
        ctx = WorkerContext(config={})
        item = {
            "old_name": "[LoliHouse] Tensei Shitara Slime Datta Ken S02E01 [WebRip 1080p HEVC-10bit AAC SRTx3].strm"
        }
        candidates = [
            {
                "id": "118541",
                "title": "The Slime Diaries",
                "alt_title": "転スラ日記 転生したらスライムだった件",
                "release": "2021-04-06",
                "meta": {
                    "original_title": "転スラ日記 転生したらスライムだった件",
                    "search_query": "Tensei Shitara Slime Datta Ken",
                    "search_rank": 1,
                },
            },
            {
                "id": "82684",
                "title": "That Time I Got Reincarnated as a Slime",
                "alt_title": "転生したらスライムだった件",
                "release": "2018-10-02",
                "meta": {
                    "original_title": "転生したらスライムだった件",
                    "search_query": "Tensei Shitara Slime Datta Ken",
                    "search_rank": 2,
                },
            },
        ]
        _title, tmdb_id, _msg, _meta = ctx._select_best_db_match(
            item,
            "Tensei Shitara Slime Datta Ken",
            None,
            True,
            "TMDb",
            candidates,
        )
        self.assertEqual(tmdb_id, "82684")

    def test_tmdb_requested_variant_candidate_is_allowed(self):
        ctx = WorkerContext(config={})
        item = {
            "old_name": "[LoliHouse] The Slime Diaries S01E01 [WebRip 1080p].strm"
        }
        candidates = [
            {
                "id": "118541",
                "title": "The Slime Diaries",
                "alt_title": "転スラ日記 転生したらスライムだった件",
                "release": "2021-04-06",
                "meta": {
                    "original_title": "転スラ日記 転生したらスライムだった件",
                    "search_query": "The Slime Diaries",
                    "search_rank": 1,
                },
            }
        ]
        _title, tmdb_id, _msg, _meta = ctx._select_best_db_match(
            item,
            "The Slime Diaries",
            None,
            True,
            "TMDb",
            candidates,
        )
        self.assertEqual(tmdb_id, "118541")

    def test_extract_explicit_season_from_sxxeyy(self):
        name = "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC.strm"
        self.assertEqual(extract_explicit_season(name), 1)

    def test_pick_season_ignores_zero_fallback(self):
        season = pick_season("Extracurricular.E01.2020", {}, 0)
        self.assertEqual(season, 1)

    def test_pick_season_uses_explicit_over_zero_guess(self):
        season = pick_season("Extracurricular.S01E01.2020", {"season": 0}, 0)
        self.assertEqual(season, 1)

    def test_special_tag_regex_does_not_match_extracurricular(self):
        name = "Extracurricular.S01E01.2020.NF.WEB-DL.1080p.HEVC"
        self.assertIsNone(SPECIAL_TAG_RE.search(name))

    def test_special_tag_regex_matches_real_special_marker(self):
        name = "Anime.Title.S01E01.[NC.Ver].1080p"
        self.assertIsNotNone(SPECIAL_TAG_RE.search(name))

    def test_guessit_assist_detects_group_release_style(self):
        g = {"title": "Dungeon Meshi"}
        self.assertTrue(
            _guessit_needs_assist(
                "[KTXP][Dungeon Meshi][01][CHS][1080P][AVC]",
                r"D:\Anime\Dungeon Meshi",
                g,
                "Dungeon Meshi",
                1,
            )
        )

    def test_guessit_assist_skips_clean_standard_name(self):
        g = {"title": "The Mandalorian", "episode": 4}
        self.assertFalse(
            _guessit_needs_assist(
                "The.Mandalorian.S03E04.2023.WEB-DL",
                r"D:\TV\The Mandalorian\Season 3",
                g,
                "The Mandalorian",
                4,
            )
        )

    def test_is_meaningful_title_rejects_generic_values(self):
        self.assertFalse(_is_meaningful_title("未知"))
        self.assertFalse(_is_meaningful_title("Season 1"))
        self.assertTrue(_is_meaningful_title("Violet Evergarden"))

    def test_is_ai_rate_limited_error_detects_429(self):
        self.assertTrue(is_ai_rate_limited_error("429 Too Many Requests"))
        self.assertTrue(is_ai_rate_limited_error("temporarily rate-limited upstream"))
        self.assertFalse(is_ai_rate_limited_error("network timeout"))

    def test_normalize_parse_source_maps_hybrid_to_ai(self):
        self.assertEqual(normalize_parse_source("hybrid"), "ai")
        self.assertEqual(normalize_parse_source("guessit"), "guessit")


    def test_guessit_assist_skips_clean_standard_name_in_localized_season_dir(self):
        g = {
            "title": "Frieren Beyond Journeys End",
            "season": 1,
            "episode": 1,
            "type": "episode",
        }
        self.assertFalse(
            _guessit_needs_assist(
                "Frieren.Beyond.Journeys.End.S01E01.2023.1080p.BluRay.Remux",
                r"Y:\STRM\动漫刮削好的\葬送的芙莉莲（2023）\Season 1",
                g,
                "Frieren Beyond Journeys End",
                1,
            )
        )

    def test_can_reuse_dir_ai_accepts_cached_alias_title(self):
        cached_ai = {
            "title": "葬送的芙莉莲",
            "title_aliases": ["Frieren Beyond Journeys End"],
            "year": 2023,
        }
        guess_data = {"title": "Frieren Beyond Journeys End", "year": 2023}
        self.assertTrue(
            can_reuse_dir_ai(
                cached_ai,
                "Frieren.Beyond.Journeys.End.S01E02.2023.1080p.BluRay.Remux",
                guess_data,
            )
        )

    def test_same_folder_season_cache_reuses_even_when_title_guess_is_noisy(self):
        cached_ai = {
            "title": "想要成为影之实力者！",
            "year": 2023,
            "season": 1,
            "cache_season": 1,
        }
        guess_data = {"title": "Nekomo kissaten VCB Studio Ma10p flac JPTC"}
        self.assertTrue(
            _can_reuse_same_folder_season_cache(cached_ai, 1, guess_data)
        )

    def test_same_folder_season_cache_does_not_cross_seasons(self):
        cached_ai = {
            "title": "想要成为影之实力者！",
            "season": 2,
            "cache_season": 1,
        }
        self.assertFalse(_can_reuse_same_folder_season_cache(cached_ai, 2, {}))
        self.assertTrue(_can_reuse_same_folder_season_cache(cached_ai, 1, {}))
        self.assertNotEqual(
            _dir_cache_key(r"D:\Anime\Show", 1),
            _dir_cache_key(r"D:\Anime\Show", 2),
        )

    def test_auto_pick_candidate_uses_general_metadata_confidence(self):
        candidates = [
            {
                "title": "想要成为影之实力者！",
                "alt_title": "陰の実力者になりたくて！",
                "id": "119495",
                "rating": 8.0,
                "release": "2022-10-05",
                "meta": {
                    "overview": "A boy dreams of becoming a hidden mastermind.",
                    "poster": "/poster.jpg",
                    "original_title": "陰の実力者になりたくて！",
                },
            },
            {
                "title": "Karens Kagebord",
                "alt_title": "",
                "id": "bad",
                "rating": 0,
                "release": "",
                "meta": {"poster": "/poster2.jpg"},
            },
        ]
        picked, reason = auto_pick_candidate_by_score(
            "Kage no Jitsuryokusha ni Naritakute!", None, "TMDb", candidates
        )
        self.assertIs(picked, candidates[0])
        self.assertIn("高置信", reason)

    def test_auto_pick_candidate_refuses_low_confidence_close_candidates(self):
        candidates = [
            {"title": "Alpha", "id": "1", "rating": 0, "release": "", "meta": {}},
            {"title": "Alpine", "id": "2", "rating": 0, "release": "", "meta": {}},
        ]
        picked, reason = auto_pick_candidate_by_score(
            "Alp", None, "TMDb", candidates
        )
        self.assertIsNone(picked)
        self.assertIn("自动评分不足", reason)

    def test_parse_candidate_pick_response_accepts_loose_json(self):
        parsed = _parse_candidate_pick_response('{"pick": 1 "reason": "first"}')
        self.assertEqual(parsed["pick"], 1)
        self.assertEqual(parsed["reason"], "first")

    def test_parse_candidate_pick_response_accepts_plain_pick(self):
        parsed = _parse_candidate_pick_response("pick: 2 reason: \"better title\"")
        self.assertEqual(parsed["pick"], 2)
        self.assertEqual(parsed["reason"], "better title")

    def test_folder_watcher_serializes_same_directory(self):
        watcher = FolderWatcher()
        first = watcher._acquire_dir_slot(r"Y:\test\show\S01E01.mkv")
        acquired_second = threading.Event()
        second_holder = {}

        def worker():
            second_holder["slot"] = watcher._acquire_dir_slot(r"Y:\test\show\S01E02.mkv")
            acquired_second.set()

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        time.sleep(0.2)
        self.assertFalse(acquired_second.is_set())
        watcher._release_dir_slot(first)
        t.join(timeout=2)
        self.assertTrue(acquired_second.is_set())
        watcher._release_dir_slot(second_holder["slot"])


if __name__ == "__main__":
    unittest.main()
