import unittest

from utils.value_utils import years_match_exact, years_within_tolerance
from core.services.db_match_service import (
    _has_cjk,
    _is_substantial_query_norm,
    pick_strong_tmdb_direct_hit,
)
from core.services.candidate_picker_service import (
    _year_diff,
    auto_pick_candidate_by_score,
)


def make_candidate(title, year=None, rank=1, rating=8.0, **meta):
    m = {"search_rank": rank, "search_query": title}
    m.update(meta)
    return {
        "title": title,
        "alt_title": meta.get("alt_title", ""),
        "id": meta.get("id", "100"),
        "rating": rating,
        "release": f"{year}-01-01" if year else "",
        "meta": m,
    }


class YearHelpers(unittest.TestCase):
    def test_within_tolerance_off_by_one(self):
        self.assertTrue(years_within_tolerance("2020", "2021"))
        self.assertTrue(years_within_tolerance("2020-04-01", "2019"))

    def test_within_tolerance_far(self):
        self.assertFalse(years_within_tolerance("2020", "2023"))

    def test_within_tolerance_missing_side(self):
        self.assertTrue(years_within_tolerance("", "2021"))
        self.assertTrue(years_within_tolerance("2021", ""))

    def test_exact(self):
        self.assertTrue(years_match_exact("2020-04", "2020"))
        self.assertFalse(years_match_exact("2020", "2021"))
        self.assertFalse(years_match_exact("", "2020"))

    def test_year_diff(self):
        self.assertEqual(_year_diff("2020", "2021"), 1)
        self.assertEqual(_year_diff("2020", "2020"), 0)
        self.assertIsNone(_year_diff("", "2020"))


class CjkShortTitle(unittest.TestCase):
    def test_has_cjk(self):
        self.assertTrue(_has_cjk("咒术回战"))
        self.assertTrue(_has_cjk("ワンピース"))
        self.assertFalse(_has_cjk("Frieren"))

    def test_substantial_norm(self):
        # CJK as short as 2 chars qualifies
        self.assertTrue(_is_substantial_query_norm("咒术"))
        # Latin still needs >= 6
        self.assertFalse(_is_substantial_query_norm("abc"))
        self.assertTrue(_is_substantial_query_norm("frieren"))
        self.assertFalse(_is_substantial_query_norm(""))

    def test_direct_hit_allows_cjk_short_title(self):
        cands = [make_candidate("咒术回战", year="2020", rank=1)]
        hit, q = pick_strong_tmdb_direct_hit(["咒术回战"], "2020", cands)
        self.assertIsNotNone(hit)

    def test_direct_hit_year_off_by_one_ok(self):
        cands = [make_candidate("咒术回战", year="2021", rank=1)]
        hit, _ = pick_strong_tmdb_direct_hit(["咒术回战"], "2020", cands)
        self.assertIsNotNone(hit)

    def test_direct_hit_year_far_rejected(self):
        cands = [make_candidate("咒术回战", year="2025", rank=1)]
        hit, _ = pick_strong_tmdb_direct_hit(["咒术回战"], "2020", cands)
        self.assertIsNone(hit)


class ScoringYearTolerance(unittest.TestCase):
    def test_off_by_one_not_overpenalized(self):
        # exact-year wrong title vs off-by-one correct title
        correct = make_candidate("Frieren", year="2024", rank=2, id="1",
                                  overview="x", poster="p", original_title="Frieren")
        wrong = make_candidate("Totally Other", year="2023", rank=1, id="2",
                               overview="y", poster="q", original_title="Totally Other")
        pick, reason = auto_pick_candidate_by_score("Frieren", "2023", "TMDb",
                                                     [wrong, correct])
        # The correct title should win despite being off-by-one on the year.
        self.assertIsNotNone(pick)
        self.assertEqual(pick["id"], "1")


if __name__ == "__main__":
    unittest.main()
