from core.metadata.sidecar_update_service import (
    refresh_sidecar_files,
    write_sidecar_files,
)
from core.services.db_match_service import (
    pick_strong_tmdb_direct_hit,
    resolve_db_match,
    select_best_db_match,
)
from core.services.matcher_service import auto_pick_candidate_by_score
from core.services.worker_context_ai import (
    can_use_embedding_rank,
    can_use_ollama_for_pick,
    can_use_online_model_for_pick,
    get_runtime_embedding,
    parse_with_ollama_runtime,
    pick_candidate_with_ollama_runtime,
    pick_candidate_with_online_model_runtime,
    rerank_candidates_with_runtime_embedding,
)


class WorkerContextRuntimeMixin:
    def _parse_with_ollama(self, filename):
        return parse_with_ollama_runtime(self, filename)

    def _can_use_ollama_for_pick(self):
        return can_use_ollama_for_pick(self)

    def _can_use_online_model_for_pick(self):
        return can_use_online_model_for_pick(self)

    def _can_use_embedding_rank(self):
        return can_use_embedding_rank(self)

    def _get_embedding(self, text):
        return get_runtime_embedding(self, text)

    def _pick_strong_tmdb_direct_hit(self, query_titles, year, candidates):
        return pick_strong_tmdb_direct_hit(query_titles, year, candidates)

    def _resolve_db_match(self, item, query_title, year, is_tv, mode, ai_data, g):
        return resolve_db_match(self, item, query_title, year, is_tv, mode, ai_data, g)

    def _select_best_db_match(
        self, item, query_title, year, is_tv, source_name, candidates, recognized_title=None
    ):
        return select_best_db_match(
            self,
            item,
            query_title,
            year,
            is_tv,
            source_name,
            candidates,
            recognized_title=recognized_title,
        )

    def _rerank_candidates_with_embedding(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        return rerank_candidates_with_runtime_embedding(
            self, item, query_title, year, is_tv, source_name, candidates
        )

    def _auto_pick_candidate_by_score(self, query_title, year, source_name, candidates):
        return auto_pick_candidate_by_score(query_title, year, source_name, candidates)

    def _pick_candidate_with_ollama(self, item, query_title, year, is_tv, source_name, candidates):
        return pick_candidate_with_ollama_runtime(
            self, item, query_title, year, is_tv, source_name, candidates
        )

    def _pick_candidate_with_online_model(
        self, item, query_title, year, is_tv, source_name, candidates
    ):
        return pick_candidate_with_online_model_runtime(
            self, item, query_title, year, is_tv, source_name, candidates
        )

    def _request_manual_candidate_choice(
        self, item, query_title, source_name, candidates, recognized_title=None
    ):
        return None

    def _show_candidate_picker_dialog(
        self, item, query_title, source_name, candidates, result_holder, done_event
    ):
        result_holder["selected"] = None
        done_event.set()

    def _write_sidecar_files(self, item, target_path):
        return write_sidecar_files(self, item, target_path)

    def _refresh_sidecar_files(self, target_path, old_metadata, new_metadata):
        return refresh_sidecar_files(self, target_path, old_metadata, new_metadata)

    def process_task(self, i, advance_progress=True):
        from core.workers.task_runner import process_task

        process_task(self, i, advance_progress=advance_progress)

    def process_one_file(self, item, is_archive):
        from core.workers.execution_runner import process_one_file

        process_one_file(self, item, is_archive)

    def process_one_file_scrape(self, item):
        from core.workers.execution_runner import process_one_file_scrape

        process_one_file_scrape(self, item)
