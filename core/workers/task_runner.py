import logging

from core.recognition.preview_pipeline import (
    populate_preview_item,
    recognize_preview_item,
    resolve_preview_match,
)
from core.recognition.preview_ui import (
    async_batch_runner,
    bg_update_single_ui,
    run_preview_pool,
)
from core.workers.execution_runner import (
    process_one_file as execution_process_one_file,
    process_one_file_scrape as execution_process_one_file_scrape,
    run_execution as execution_run_execution,
    run_scrape_execution as execution_run_scrape_execution,
)
from utils.error_utils import ERROR_CODE_UNKNOWN, format_error_message

logger = logging.getLogger(__name__)


def process_task(gui, i, advance_progress=True):
    """Process a single preview task."""
    item = gui.file_list[i]

    try:
        if gui.preview_skip_all_event.is_set() or item.dir in gui.preview_skip_dirs:
            gui.root.after(
                0, lambda id_val=item.id: gui.tree.set(id_val, "st", "已跳过")
            )
            return

        gui.root.after(
            0, lambda id_val=item.id: gui.tree.set(id_val, "st", "识别中")
        )

        if gui.preview_skip_all_event.is_set() or item.dir in gui.preview_skip_dirs:
            gui.root.after(
                0, lambda id_val=item.id: gui.tree.set(id_val, "st", "已跳过")
            )
            return
        state = recognize_preview_item(gui, item)
        if not state:
            return
        match_state = resolve_preview_match(gui, item, state)
        if not match_state:
            return
        populate_preview_item(gui, item, state, match_state, i)
    except Exception as ex:
        logging.error(f"处理文件 {item.old_name} 时出错: {ex}")
        err_msg = format_error_message(ERROR_CODE_UNKNOWN, f"异常: {str(ex)[:50]}")
        gui.root.after(
            0,
            lambda id_val=item.id,
            old_name=item.old_name,
            msg=err_msg: gui.tree.item(
                id_val,
                values=(
                    old_name,
                    "错误",
                    "None",
                    gui._friendly_status_text(msg),
                    "崩溃",
                ),
            ),
        )
    finally:
        if advance_progress:
            gui.root.after(0, lambda: gui.pbar.step(1))


def run_execution(gui, is_archive):
    """Run rename/archive execution with background worker pool."""
    return execution_run_execution(gui, is_archive)


def process_one_file(gui, item, is_archive):
    """Process single file move/rename and sidecar writing."""
    return execution_process_one_file(gui, item, is_archive)


def run_scrape_execution(gui):
    """Run scrape-only execution with background worker pool."""
    return execution_run_scrape_execution(gui)


def process_one_file_scrape(gui, item):
    """Process single file scrape-only (write NFO and download images)."""
    return execution_process_one_file_scrape(gui, item)
