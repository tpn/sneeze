import os
from collections.abc import Iterable


class Dict(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        try:
            del self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class Options(Dict):
    pass


class DecayDict(Dict):
    def __getattr__(self, name):
        return self.pop(name, None)

    def get(self, name, default=None):
        return self.pop(name, default)

    def assert_empty(self, obj=None):
        if self:
            target = obj.__class__.__name__ if obj else "object"
            keys = ", ".join(sorted(self))
            raise TypeError(f"unknown {target} keyword(s): {keys}")


class Constant:
    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise AttributeError(f"constant already set: {name}")
        super().__setattr__(name, value)


def iterable(obj):
    if isinstance(obj, str) or not isinstance(obj, Iterable):
        return (obj,)
    return obj


def ensure_unique(items):
    seen = set()
    for item in items:
        if item in seen:
            raise ValueError(f"duplicate item: {item}")
        seen.add(item)
    return items


def add_linesep_if_missing(text):
    text = "" if text is None else str(text)
    return text if text.endswith(os.linesep) else text + os.linesep


def prepend_error_if_missing(text):
    text = add_linesep_if_missing(text)
    return text if text.startswith("error: ") else f"error: {text}"


def prepend_warning_if_missing(text):
    text = add_linesep_if_missing(text)
    return text if text.startswith("warning: ") else f"warning: {text}"


def strip_linesep_if_present(text):
    text = "" if text is None else str(text)
    return text[:-1] if text.endswith(os.linesep) else text
