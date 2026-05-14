import importlib
import importlib.metadata
import os
import pkgutil
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

PLUGIN_ENTRY_POINT_GROUP = "sneeze.plugins"
PLUGIN_DIST_PREFIX = "sneeze-plugin-"
PLUGIN_PACKAGE_PREFIX = "sneeze."
USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class PluginError(Exception):
    pass


@dataclass(frozen=True)
class PluginSpec:
    username: str
    package: str
    source: str


def validate_username(username):
    if not USERNAME_RE.match(username):
        raise PluginError(
            "plugin username must start with a letter or number and contain "
            "only letters, numbers, '-' or '_'"
        )
    return username.replace("-", "_")


def plugin_dist_name(username):
    return f"{PLUGIN_DIST_PREFIX}{username.replace('_', '-')}"


def plugin_package_name(username):
    return f"{PLUGIN_PACKAGE_PREFIX}{username.replace('-', '_')}"


def default_plugin_dir(username, base_dir=None):
    base = Path(base_dir or Path.home() / "src")
    return base / plugin_dist_name(username)


def github_shorthand_to_url(spec):
    if not spec.startswith("gh:"):
        return spec
    repo = spec[3:]
    if "/" not in repo:
        raise PluginError("gh: plugin specs must be in owner/repo form")
    return f"git+https://github.com/{repo}.git"


def resolve_plugin_install_target(spec, src_dir=None):
    if os.sep in spec or spec.startswith((".", "~")):
        return os.path.abspath(os.path.expanduser(spec))
    if spec.startswith(("gh:", "git+", "http://", "https://")):
        return github_shorthand_to_url(spec)

    username = validate_username(spec)
    local_dir = default_plugin_dir(username, base_dir=src_dir)
    if local_dir.exists():
        return os.fspath(local_dir)
    return github_shorthand_to_url(
        f"gh:{username.replace('_', '-')}/{plugin_dist_name(username)}"
    )


def iter_entry_point_plugins():
    try:
        entry_points = importlib.metadata.entry_points(
            group=PLUGIN_ENTRY_POINT_GROUP
        )
    except TypeError:
        entry_points = importlib.metadata.entry_points().get(
            PLUGIN_ENTRY_POINT_GROUP,
            [],
        )
    for ep in sorted(entry_points, key=lambda item: item.name):
        module_name = ep.value.split(":", 1)[0]
        yield PluginSpec(ep.name, module_name, "entry-point")


def iter_namespace_plugins():
    import sneeze

    for info in pkgutil.iter_modules(sneeze.__path__, prefix="sneeze."):
        if not info.ispkg:
            continue
        parts = info.name.split(".")
        if len(parts) != 2:
            continue
        username = parts[-1]
        yield PluginSpec(username, info.name, "namespace")


def discover_plugins():
    plugins = {}
    for spec in iter_entry_point_plugins():
        plugins[spec.username] = spec
    for spec in iter_namespace_plugins():
        plugins.setdefault(spec.username, spec)
    return [plugins[key] for key in sorted(plugins)]


def import_plugin_modules(spec):
    commands_module = importlib.import_module(f"{spec.package}.commands")
    try:
        config_module = importlib.import_module(f"{spec.package}.config")
    except ModuleNotFoundError:
        config_module = importlib.import_module("sneeze.config")
    return commands_module, config_module


def pip_install_plugin(target, editable=True):
    args = ["python", "-m", "pip", "install"]
    if editable and os.path.isdir(target):
        args.append("-e")
    args.append(target)
    return subprocess.run(args, check=False)


def pip_uninstall_plugin(name):
    dist = (
        name
        if name.startswith(PLUGIN_DIST_PREFIX)
        else plugin_dist_name(name)
    )
    return subprocess.run(
        ["python", "-m", "pip", "uninstall", "-y", dist],
        check=False,
    )


def write_text_if_needed(path, text, force=False):
    path = Path(path)
    if path.exists() and not force:
        raise PluginError(f"{path} already exists; use --force to replace it")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def scaffold_plugin(username, output_dir=None, force=False, init_git=True):
    username = validate_username(username)
    dist_name = plugin_dist_name(username)
    package = plugin_package_name(username)
    path = Path(output_dir) if output_dir else default_plugin_dir(username)
    package_dir = path / "src" / "sneeze" / username

    readme = f"# {dist_name}\n\nPlugin package for `sneeze.{username}`.\n"
    files = {
        ".gitignore": _plugin_gitignore(),
        "README.md": readme,
        "AGENTS.md": _plugin_agents_md(username),
        "pyproject.toml": _plugin_pyproject(dist_name, package),
        f"src/sneeze/{username}/__init__.py": '__version__ = "0.1"\n',
        f"src/sneeze/{username}/config.py": _plugin_config_py(),
        f"src/sneeze/{username}/commands.py": _plugin_commands_py(username),
        "tests/test_plugin_import.py": _plugin_test_py(package),
    }
    path.mkdir(parents=True, exist_ok=True)
    package_dir.mkdir(parents=True, exist_ok=True)
    for relpath, text in files.items():
        write_text_if_needed(path / relpath, text, force=force)

    if init_git and not (path / ".git").exists():
        subprocess.run(["git", "init"], cwd=path, check=False)
    return path


def _plugin_gitignore():
    return """__pycache__/
*.py[codz]
*.egg-info/
.eggs/
build/
dist/
.cache/
.coverage
.mypy_cache/
.pytest_cache/
.ruff_cache/
.venv/
"""


def _plugin_pyproject(dist_name, package):
    username = package.rsplit(".", 1)[-1]
    return f"""[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{dist_name}"
version = "0.1"
description = "Sneeze plugin for {username}."
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
  "sneeze",
]

[project.entry-points."{PLUGIN_ENTRY_POINT_GROUP}"]
{username} = "{package}"

[tool.setuptools]
package-dir = {{"" = "src"}}

[tool.setuptools.packages.find]
where = ["src"]
include = ["sneeze*"]
namespaces = true

[tool.black]
line-length = 78
target-version = ["py312"]

[tool.ruff]
line-length = 78
target-version = "py312"
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]

[tool.ruff.lint.pycodestyle]
max-line-length = 78
"""


def _plugin_agents_md(username):
    return f"""# Sneeze Plugin Agent Notes

This repository is a Sneeze plugin for `{username}`.

- Core plugin guidance lives in `../sneeze/agents/PLUGINS.md`.
- On Trent's machines, the absolute fallback is
  `~/src/sneeze/agents/PLUGINS.md`.
- Keep generic framework changes in `~/src/sneeze`; keep user-specific
  commands and dependencies in this plugin.
"""


def _plugin_config_py():
    return 'from sneeze.config import Config\n\n__all__ = ["Config"]\n'


def _plugin_commands_py(username):
    class_name = "".join(part.capitalize() for part in username.split("_"))
    return f'''from sneeze.commandinvariant import InvariantAwareCommand


class {class_name}PluginInfo(InvariantAwareCommand):
    """Show basic information about this plugin."""

    _shortname_ = "{username[:1]}pi"

    def run(self):
        self._out("sneeze plugin: {username}")
'''


def _plugin_test_py(package):
    return f"""import importlib


def test_plugin_package_imports():
    assert importlib.import_module("{package}")
"""
