from core.services.naming_templates import extract_lang_and_ext
from core.services.naming_templates import extract_media_suffix
from core.services.season_rules import (
    can_reuse_dir_ai,
    extract_explicit_season,
    get_version_tag,
    pick_season,
)
from core.services.status_text import build_status_text, friendly_status_text
from core.services.worker_context_media import render_media_filename, resolve_media_type
from core.services.worker_context_config import get_media_exts, get_sub_audio_exts


class WorkerContextMediaMixin:
    def get_media_exts(self):
        return get_media_exts(self)

    def get_sub_audio_exts(self):
        return get_sub_audio_exts(self)

    def extract_lang_and_ext(self, filename):
        return extract_lang_and_ext(filename, self.lang_tags.get())

    def _extract_explicit_season(self, pure_name):
        return extract_explicit_season(pure_name)

    def _pick_season(self, pure_name, guess_data=None, fallback=1):
        return pick_season(pure_name, guess_data, fallback)

    def _can_reuse_dir_ai(self, cached_ai, pure_name, guess_data=None):
        return can_reuse_dir_ai(cached_ai, pure_name, guess_data)

    def _get_version_tag(self, path):
        return get_version_tag(path)

    def _extract_media_suffix(self, filename, pure_name=None):
        return extract_media_suffix(filename, pure_name)

    def _render_media_filename(
        self,
        template,
        *,
        title="",
        year="",
        season="",
        episode="",
        ep_name="",
        ext="",
        source_filename="",
        pure_name="",
        parse_source="",
        source_provider="",
        media_id="",
        is_tv=True,
        original_title="",
        rating=0,
        genres=None,
        studios=None,
        overview="",
        ep_plot="",
        release="",
    ):
        return render_media_filename(
            self,
            template,
            title=title,
            year=year,
            season=season,
            episode=episode,
            ep_name=ep_name,
            ext=ext,
            source_filename=source_filename,
            pure_name=pure_name,
            parse_source=parse_source,
            source_provider=source_provider,
            media_id=media_id,
            is_tv=is_tv,
            original_title=original_title,
            rating=rating,
            genres=genres,
            studios=studios,
            overview=overview,
            ep_plot=ep_plot,
            release=release,
        )

    def _friendly_status_text(self, message):
        return friendly_status_text(message)

    def _build_status_text(self, *messages):
        return build_status_text(*messages)

    def _resolve_media_type(self, guess_data=None, pure_name=None, extracted_ep=None):
        return resolve_media_type(self, guess_data, pure_name, extracted_ep)
