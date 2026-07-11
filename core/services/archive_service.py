"""Shared filesystem archive operations for automatic and manual workflows."""

from dataclasses import dataclass
import os
import shutil
from typing import Callable

class ArchiveConflictError(FileExistsError):
    pass


@dataclass
class ArchiveResult:
    target: str
    operation: str
    source_removed: bool
    reused_existing: bool = False


class ArchiveService:
    @staticmethod
    def _remove_empty_dirs(start_dir: str, stop_at: str | None = None):
        current = os.path.normpath(start_dir)
        stop = os.path.normcase(os.path.normpath(stop_at)) if stop_at else None
        while os.path.isdir(current):
            if stop and os.path.normcase(current) == stop:
                break
            try:
                if os.listdir(current):
                    break
                os.rmdir(current)
            except OSError:
                break
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent

    def archive(
        self,
        item,
        *,
        target: str,
        organize_mode: str,
        write_sidecars: Callable,
        watch_root: str | None = None,
        allow_existing_target: bool = False,
        replace_broken_target: bool = False,
        on_phase: Callable | None = None,
    ) -> ArchiveResult:
        on_phase = on_phase or (lambda _phase, _details=None: None)
        on_phase("prepared", {"source": item.path, "target": target})
        target = os.path.normpath(target)
        target_dir = os.path.dirname(target)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        source = os.path.normpath(item.path)
        reused_existing = False
        operation = "in_place"
        source_removed = False

        if os.path.normcase(source) != os.path.normcase(target):
            target_exists = os.path.exists(target)
            target_lexists = os.path.lexists(target)
            same_file = False
            if target_exists and os.path.isfile(source):
                try:
                    same_file = os.path.samefile(source, target)
                except (OSError, ValueError):
                    pass

            if same_file or (allow_existing_target and target_exists):
                reused_existing = True
            elif target_lexists and replace_broken_target and not target_exists:
                os.remove(target)
                target_lexists = False
            elif target_lexists:
                raise ArchiveConflictError(f"目标文件已存在: {target}")

            if not reused_existing:
                src_dir = os.path.dirname(source)
                if organize_mode == "copy":
                    shutil.copy2(source, target)
                    operation = "copy"
                elif organize_mode == "symlink":
                    os.symlink(os.path.abspath(source), target)
                    operation = "symlink"
                elif organize_mode == "hardlink":
                    os.link(source, target)
                    operation = "hardlink"
                else:
                    shutil.move(source, target)
                    operation = "move"
                    source_removed = True
                    self._remove_empty_dirs(src_dir, stop_at=watch_root)
            item.path = target

        on_phase("file_done", {"target": target, "operation": operation})

        write_sidecars(item, target)
        on_phase("sidecars_done", {"target": target})
        return ArchiveResult(
            target=target,
            operation=operation,
            source_removed=source_removed,
            reused_existing=reused_existing,
        )


archive_service = ArchiveService()
