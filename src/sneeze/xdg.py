from __future__ import annotations

import os
from pathlib import Path


def xdg_config_home() -> Path:
    return Path(
        os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config"
    ).expanduser()


def xdg_state_home() -> Path:
    return Path(
        os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state"
    ).expanduser()


def xdg_data_home() -> Path:
    return Path(
        os.environ.get("XDG_DATA_HOME") or Path.home() / ".local" / "share"
    ).expanduser()


def xdg_cache_home() -> Path:
    return Path(
        os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache"
    ).expanduser()


def xdg_config_dir(app_slug: str) -> Path:
    return xdg_config_home() / app_slug


def xdg_state_dir(app_slug: str) -> Path:
    return xdg_state_home() / app_slug
