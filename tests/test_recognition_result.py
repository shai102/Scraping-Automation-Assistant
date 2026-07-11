import unittest

from core.models.recognition_result import RecognitionResult
from core.recognition.result_service import build_recognition_result, calculate_confidence
from utils import cache_runtime
from utils.cache_store import get_cache_key


class RecognitionResultTests(unittest.TestCase):
    def test_high_confidence_folder_id_match(self):
        state = {
            "pure": "Show.S01E02",
            "title": "Show",
            "year": 2024,
            "season": 1,
            "episode_calc": 2,
            "episode_range": None,
            "media_type": "episode",
            "is_tv": True,
            "parse_source": "guessit",
        }
        match = {
            "std_title": "Show",
            "tid": "123",
            "provider_name": "tmdb",
            "db_message": "文件夹ID锁定",
        }
        result = build_recognition_result(state, match)
        self.assertIsInstance(result, RecognitionResult)
        self.assertEqual("high", result.confidence_level)
        self.assertGreaterEqual(result.confidence, 0.85)
        self.assertEqual("database_match", result.trace[1]["stage"])

    def test_missing_match_is_low_confidence_and_explained(self):
        state = {
            "pure": "unknown",
            "title": "unknown",
            "year": None,
            "media_type": "movie",
            "is_tv": False,
            "parse_source": "ai",
        }
        match = {
            "std_title": "unknown",
            "tid": "None",
            "provider_name": "tmdb",
            "db_message": "候选存在歧义",
        }
        confidence, warnings = calculate_confidence(state, match)
        self.assertLess(confidence, 0.60)
        self.assertTrue(any("年份" in warning for warning in warnings))
        self.assertTrue(any("候选存在歧义" in warning for warning in warnings))

    def test_cache_key_contains_algorithm_version(self):
        key = get_cache_key("tmdb_detail", "123_tv")
        self.assertTrue(key.startswith(f"{cache_runtime.CACHE_ALGORITHM_VERSION}:"))


if __name__ == "__main__":
    unittest.main()
