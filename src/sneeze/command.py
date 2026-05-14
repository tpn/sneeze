import logging
import os
import re
from abc import ABCMeta, abstractmethod

from .util import (
    Dict,
    add_linesep_if_missing,
    iterable,
    prepend_error_if_missing,
    prepend_warning_if_missing,
)

COMMAND_CLASS_REGEX = re.compile("[A-Z][^A-Z]*")
STANDARD_LOG_LEVEL_NAMES = (
    "CRITICAL",
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
    "NOTSET",
)
LOG_LEVEL_ALIASES = {
    "WARN": "WARNING",
    "FATAL": "CRITICAL",
}
DEFAULT_LOG_LEVEL_NAME = "INFO"
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_LEVEL_VALUE_BY_NAME = {
    name: getattr(logging, name) for name in STANDARD_LOG_LEVEL_NAMES
}
_LEVEL_NAME_BY_VALUE = {
    value: name
    for value, name in logging._levelToName.items()
    if isinstance(value, int)
}
DEFAULT_LOG_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs")
)
DEFAULT_LOG_FILENAME = "sneeze.log"
LOG_FILE_ENV = "SNEEZE_LOG_FILE"


class CommandError(Exception):
    pass


class ClashingCommandNames(CommandError):
    def __init__(self, name, previous, this):
        super().__init__(
            "clashing command name: module "
            f"'{this}' defines '{name}', already defined by '{previous}'"
        )


class _DummyStream:
    def write(self, msg):
        return None

    def read(self):
        return None

    def flush(self):
        return None

    def close(self):
        return None


DummyStream = _DummyStream()


def _log_level_error(value):
    choices = ", ".join(STANDARD_LOG_LEVEL_NAMES)
    return ValueError(
        f"invalid log level '{value}'; expected one of: {choices}"
    )


def resolve_log_level(value):
    if value is None:
        name = DEFAULT_LOG_LEVEL_NAME
        return name, _LEVEL_VALUE_BY_NAME[name]
    if isinstance(value, int):
        name = _LEVEL_NAME_BY_VALUE.get(value)
        if name:
            return name, value
        raise _log_level_error(value)
    candidate = str(value).strip()
    if not candidate:
        name = DEFAULT_LOG_LEVEL_NAME
        return name, _LEVEL_VALUE_BY_NAME[name]
    if candidate.isdigit():
        numeric = int(candidate)
        name = _LEVEL_NAME_BY_VALUE.get(numeric)
        if name:
            return name, numeric
        raise _log_level_error(value)
    candidate = LOG_LEVEL_ALIASES.get(candidate.upper(), candidate.upper())
    numeric = _LEVEL_VALUE_BY_NAME.get(candidate)
    if numeric is None:
        raise _log_level_error(value)
    return candidate, numeric


def _resolve_log_file(path=None):
    if path:
        return path
    env_value = os.environ.get(LOG_FILE_ENV)
    if env_value:
        return env_value
    return os.path.join(DEFAULT_LOG_DIR, DEFAULT_LOG_FILENAME)


def configure_logging(level, log_format=DEFAULT_LOG_FORMAT, log_file=None):
    filename = _resolve_log_file(log_file)
    directory = os.path.dirname(os.path.abspath(filename))
    if directory:
        os.makedirs(directory, exist_ok=True)
    root = logging.getLogger()
    for handler in list(root.handlers):
        try:
            handler.flush()
        finally:
            try:
                handler.close()
            finally:
                root.removeHandler(handler)
    handler = logging.FileHandler(filename, encoding="utf-8")
    handler.setLevel(level)
    if log_format:
        handler.setFormatter(logging.Formatter(log_format))
    root.addHandler(handler)
    root.setLevel(level)
    return filename


class CommandFormatter:
    def verbose(self, msg):
        return add_linesep_if_missing(msg)

    def out(self, msg):
        return add_linesep_if_missing(msg)

    def warn(self, msg):
        return prepend_warning_if_missing(msg)

    def err(self, msg):
        return prepend_error_if_missing(msg)

    def start(self, command):
        return None

    def end(self, suppress, *exc_info):
        return None


class Command(metaclass=ABCMeta):
    __first_command__ = None
    __active_command__ = None

    _argc_ = 0
    _vargc_ = None
    _conf_ = True
    _log_level_ = True
    _usage_ = None
    _quiet_ = None
    _verbose_ = None
    _shortname_ = None
    _description_ = None
    _disable_interspersed_args_ = False

    log_level = None
    _log_level = None
    log_file = None

    def __init__(self, istream=None, ostream=None, estream=None):
        self.interactive = False
        self.istream = istream or DummyStream
        self.ostream = ostream or DummyStream
        self.estream = estream or DummyStream
        self.conf = None
        self.args = []
        self.result = None
        self.results = None
        self.options = None
        self.load_order = []
        self.cprofile = False
        self.entered = False
        self.exited = False
        self.formatter = CommandFormatter()
        self.is_first = False
        self.first = None
        self.prev = None
        self.next = None
        self._next_commands = []
        self._exit_functions = []
        self._stash = None
        self._quiet = None

        tokens = [
            t for t in COMMAND_CLASS_REGEX.findall(self.__class__.__name__)
        ]
        if tokens and tokens[-1] == "Command":
            tokens = tokens[:-1]
        self.name = "-".join(tokens).lower()
        if self._shortname_ is not None:
            self.shortname = self._shortname_
        elif len(tokens) > 1:
            self.shortname = "".join(t[0] for t in tokens).lower()
        else:
            self.shortname = self.name

    def __enter__(self):
        if not Command.__active_command__:
            Command.__first_command__ = self
            self.is_first = True
            self.first = self
            self._stash = Dict()
            self._first_enter()
        else:
            active = Command.__active_command__
            self.first = Command.__first_command__
            self.prev = active
            active.next = self
        Command.__active_command__ = self
        self.entered = True
        self._allocate()
        self.formatter.start(self)
        return self

    def __exit__(self, *exc_info):
        self.exited = True
        suppress = self._deallocate(*exc_info)
        self.formatter.end(suppress, *exc_info)
        Command.__active_command__ = self.prev
        if not self.prev:
            self._first_exit()
            Command.__first_command__ = None
        for fn, args, kwds in self._exit_functions:
            fn(*args, **kwds)
        return suppress

    @property
    def is_quiet(self):
        if self._quiet is not None:
            return self._quiet
        if self.options is None:
            return False
        return bool(getattr(self.options, "quiet", False))

    @property
    def stash(self):
        return self._stash if self.is_first else self.first._stash

    def _flush(self):
        self.ostream.flush()
        self.estream.flush()

    def _first_enter(self):
        return None

    def _first_exit(self):
        return None

    def _allocate(self):
        return None

    def _deallocate(self, *exc_info):
        return False

    def _verbose(self, msg):
        if getattr(self.options, "verbose", False):
            self.ostream.write(self.formatter.verbose(msg))
            self._flush()

    def _out(self, msg):
        if not self.is_quiet:
            self.ostream.write(self.formatter.out(msg))
            self._flush()

    def _warn(self, msg):
        self.ostream.write(self.formatter.warn(msg))
        self._flush()

    def _err(self, msg):
        self.estream.write(self.formatter.err(msg))
        self._flush()

    def _pre_load_options(self):
        return None

    def _load_early_options(self):
        return None

    def _load_options(self):
        opts = self.options or {}
        keys = set(opts.keys())
        if hasattr(self, "_invariant_order"):
            attrs = [
                name for _, name in self._invariant_order if name in keys
            ]
        else:
            attrs = [
                name
                for name in keys
                if hasattr(self, name) and not name.startswith("_")
            ]
        order = []
        for attr in attrs:
            value = getattr(opts, attr)
            if value is not None:
                setattr(self, attr, value)
                order.append(attr)
        self.load_order = order

    def _pre_enter(self):
        return None

    def _pre_run(self):
        return None

    @abstractmethod
    def run(self):
        pass

    def _post_run(self):
        return None

    def _run_next(self):
        seen = set()
        for cls in iterable(self._next_commands):
            if cls in seen:
                continue
            command = self.prime(cls)
            command.start()
            seen.add(cls)

    def start(self):
        self._load_early_options()
        self._pre_enter()
        with self:
            self._pre_load_options()
            self._load_options()
            self._pre_run()
            self.run()
            self._post_run()
            self._run_next()
        self._end()

    def _end(self):
        return None

    def prime(self, cls):
        command = cls(self.istream, self.ostream, self.estream)
        command.conf = self.conf
        command.log_level = self.log_level
        command._log_level = self._log_level
        command.log_file = self.log_file
        command.options = self.options
        return command

    def set_log_level(self, value):
        name, numeric = resolve_log_level(value)
        self.log_level = name
        self._log_level = numeric
        self.log_file = configure_logging(numeric)
        return numeric

    @classmethod
    def get_active_command(cls):
        return Command.__active_command__

    @classmethod
    def get_first_command(cls):
        return Command.__first_command__

    def on_exit(self, fn, *args, **kwds):
        self._exit_functions.append((fn, args, kwds))
