from typing import Any


class SimpleVar:
    """Minimal replacement for tkinter StringVar / BooleanVar."""

    def __init__(self, value: Any = ""):
        self._value = value

    def get(self):
        return self._value

    def set(self, value):
        self._value = value


class DummyRoot:
    """Absorbs gui.root.after(0, fn) by just calling fn immediately in-thread."""

    def __init__(self, ctx):
        self._ctx = ctx

    def after(self, delay_ms, fn=None, *args):
        if fn is not None:
            try:
                fn()
            except Exception:
                pass


class DummyTree:
    """Translates legacy tree updates into headless status events."""

    def __init__(self, ctx=None):
        self._ctx = ctx

    def set(self, item_id, column=None, value=None, *args, **kwargs):
        if self._ctx and column in ("st", "status"):
            self._ctx.emit_status(item_id, str(value or ""), {"source": "legacy_tree_set"})

    def item(self, item_id, *args, **kwargs):
        values = kwargs.get("values")
        if self._ctx and isinstance(values, (tuple, list)) and values:
            self._ctx.emit_status(
                item_id,
                str(values[-1] or ""),
                {"source": "legacy_tree_item", "values": list(values)},
            )


class DummyProgressbar:
    def step(self, *args):
        pass

    def config(self, **kwargs):
        pass

    def configure(self, **kwargs):
        pass


class DummyLabel:
    def config(self, **kwargs):
        pass


class DummyButton:
    def config(self, **kwargs):
        pass
