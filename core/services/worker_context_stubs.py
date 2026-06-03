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
    """Absorbs gui.tree.set() and gui.tree.item() calls."""

    def set(self, *args, **kwargs):
        pass

    def item(self, *args, **kwargs):
        pass


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
