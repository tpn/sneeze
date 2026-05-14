import inspect
import os
import socket
from configparser import RawConfigParser
from os.path import abspath, basename, dirname, expanduser, expandvars

from .path import join_path
from .util import Options, classproperty

CONFIG = None
CONFIG_CLASS = None

PATH = dirname(abspath(__file__))
NAMESPACE = basename(PATH)

XDG_DATA_HOME = os.environ.get(
    "XDG_DATA_HOME",
    join_path(expanduser("~"), ".local/share"),
)
XDG_STATE_HOME = os.environ.get(
    "XDG_STATE_HOME",
    join_path(expanduser("~"), ".local/state"),
)
XDG_CONFIG_HOME = os.environ.get(
    "XDG_CONFIG_HOME",
    join_path(expanduser("~"), ".config"),
)

SNEEZE_DATA_DIR = os.environ.get(
    "SNEEZE_DATA_DIR",
    join_path(XDG_DATA_HOME, "sneeze"),
)
SNEEZE_CONF_DIR = os.environ.get(
    "SNEEZE_CONF_DIR",
    join_path(XDG_CONFIG_HOME, "sneeze"),
)
SNEEZE_RUN_DIR = os.environ.get(
    "SNEEZE_RUN_DIR",
    join_path(XDG_STATE_HOME, "sneeze/run"),
)

HOSTFQDN = (socket.getfqdn() or socket.gethostname() or "localhost").lower()
HOSTNAME = HOSTFQDN.split(".")[0]


class ConfigError(Exception):
    pass


class NoConfigObjectCreated(Exception):
    pass


class ConfigObjectAlreadyCreated(Exception):
    pass


class ConfigClassAlreadySet(Exception):
    pass


def get_config():
    global CONFIG
    if CONFIG is None:
        raise NoConfigObjectCreated()
    return CONFIG


def get_or_create_config():
    try:
        return get_config()
    except NoConfigObjectCreated:
        conf = Config()
        conf.load()
        return conf


def _clear_config_if_already_created():
    global CONFIG
    CONFIG = None


def set_config_class(cls):
    global CONFIG_CLASS
    if CONFIG_CLASS is not None:
        raise ConfigClassAlreadySet()
    CONFIG_CLASS = cls


def _clear_config_class_if_already_set():
    global CONFIG_CLASS
    CONFIG_CLASS = None


class Config(RawConfigParser):
    def __init__(self, options=None):
        super().__init__()
        self.optionxform = str
        self.options = options if options else Options()
        self.hostname = HOSTFQDN
        self.shortname = HOSTNAME
        self.files = None
        self.filename = None

        global CONFIG
        if CONFIG is not None:
            raise ConfigObjectAlreadyCreated()
        CONFIG = self

    @classproperty
    def namespace(cls):
        return basename(dirname(inspect.getsourcefile(cls)))

    @classmethod
    def _resolve_dir(cls, name):
        path = inspect.getsourcefile(cls)
        base = dirname(join_path(path, "../.."))
        return join_path(base, name)

    @classproperty
    def conf_dir(cls):
        return cls._resolve_dir("conf")

    @classproperty
    def data_dir(cls):
        path = cls._resolve_dir("data")
        os.makedirs(path, exist_ok=True)
        return path

    def _absdir(self, name, section="main"):
        value = self.get(section, name)
        if not value:
            return None
        for _ in range(10):
            expanded = expandvars(expanduser(value))
            if expanded == value:
                return abspath(expanded)
            value = expanded
        raise RuntimeError(f"exceeded path expansion depth for {name}")

    def load(self, filename=None):
        self.filename = filename
        if filename:
            self.files = self.read(filename)
        else:
            self.files = []
        return self
