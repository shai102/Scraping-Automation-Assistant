import datetime
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.metadata.completeness import metadata_is_incomplete, metadata_missing_fields
from monitor import metadata_refresh_runner


def _episode_meta(**overrides):
    meta = {
        "id": "285838",
        "provider": "tmdb",
        "type": "episode",
        "title": "夺命许愿",
        "ep_title": "第一集",
        "ep_plot": "剧情简介",
        "still": "still.jpg",
        "overview": "作品简介",
        "actors": ["演员"],
        "genres": ["剧情"],
        "rating": 7.0,
    }
    meta.update(overrides)
    return json.dumps(meta, ensure_ascii=False)


class MetadataCompletenessPolicyTests(unittest.TestCase):
    def test_chinese_generic_episode_title_counts_as_missing_by_default(self):
        missing = metadata_missing_fields(_episode_meta())

        self.assertIn("集标题", missing)

    def test_episode_title_ignore_rule_can_match_title_or_provider_id(self):
        for rules in ("夺命许愿", "tmdb:285838"):
            with self.subTest(rules=rules):
                missing = metadata_missing_fields(
                    _episode_meta(),
                    ignore_episode_title_rules=rules,
                    title_hint="夺命许愿",
                    matched_id="285838",
                )

                self.assertNotIn("集标题", missing)
                self.assertFalse(
                    metadata_is_incomplete(
                        _episode_meta(),
                        ignore_episode_title_rules=rules,
                        title_hint="夺命许愿",
                        matched_id="285838",
                    )
                )

    def test_skip_rule_suppresses_all_missing_fields_for_selected_work(self):
        incomplete = _episode_meta(
            ep_title="第一集",
            ep_plot="",
            still="",
            overview="",
            actors=[],
            genres=[],
            rating=0,
        )

        self.assertTrue(metadata_is_incomplete(incomplete))
        for rules in ("夺命许愿", "tmdb:285838"):
            with self.subTest(rules=rules):
                self.assertEqual(
                    [],
                    metadata_missing_fields(
                        incomplete,
                        skip_rules=rules,
                        title_hint="夺命许愿",
                        matched_id="285838",
                    ),
                )
                self.assertFalse(
                    metadata_is_incomplete(
                        incomplete,
                        skip_rules=rules,
                        title_hint="夺命许愿",
                        matched_id="285838",
                    )
                )

    def test_skip_rule_can_match_provider_hint_when_metadata_lacks_provider(self):
        incomplete = _episode_meta(provider="")

        self.assertFalse(
            metadata_is_incomplete(
                incomplete,
                skip_rules="tmdb:285838",
                matched_id="285838",
                provider_hint="tmdb",
            )
        )


class MetadataRefreshBackoffTests(unittest.TestCase):
    def test_no_progress_attempt_sets_backoff_and_next_pass_skips(self):
        now = datetime.datetime.now()
        state = SimpleNamespace(
            record_id=10,
            attempts=0,
            no_progress_count=0,
            last_missing_fields=None,
            last_error=None,
            last_attempt_at=None,
            next_attempt_at=None,
            created_at=now,
            updated_at=now,
        )
        session = SimpleNamespace(commits=0)

        def commit():
            session.commits += 1

        session.commit = commit
        with patch.object(metadata_refresh_runner, "_get_or_create_state", return_value=state):
            metadata_refresh_runner._mark_refresh_result(
                session,
                10,
                before_missing=["集标题"],
                after_missing=["集标题"],
                updated=False,
                error=None,
                now=now,
            )

        self.assertEqual(1, state.attempts)
        self.assertEqual(1, state.no_progress_count)
        self.assertGreater(state.next_attempt_at, now)
        self.assertEqual(1, session.commits)


if __name__ == "__main__":
    unittest.main()
