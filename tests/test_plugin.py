import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import sneeze
from sneeze.plugin import (
    PluginSpec,
    github_shorthand_to_url,
    import_plugin_modules,
    pip_install_plugin,
    pip_uninstall_plugin,
    plugin_dist_name,
    plugin_package_name,
    resolve_plugin_install_target,
    scaffold_plugin,
)


def test_plugin_names():
    assert plugin_dist_name("tpn") == "sneeze-plugin-tpn"
    assert plugin_package_name("tpn") == "sneeze.tpn"


def test_github_shorthand_to_url():
    assert github_shorthand_to_url("gh:tpn/sneeze-plugin-tpn") == (
        "git+https://github.com/tpn/sneeze-plugin-tpn.git"
    )


def test_install_target_prefers_local_sibling(tmp_path):
    local = tmp_path / "sneeze-plugin-tpn"
    local.mkdir()

    target = resolve_plugin_install_target("tpn", src_dir=str(tmp_path))

    assert target == str(local)


def test_install_target_falls_back_to_github():
    target = resolve_plugin_install_target("tpn", src_dir="/does/not/exist")

    assert target == "git+https://github.com/tpn/sneeze-plugin-tpn.git"


def test_install_target_accepts_explicit_github_shorthand():
    target = resolve_plugin_install_target("gh:tpn/sneeze-plugin-tpn")

    assert target == "git+https://github.com/tpn/sneeze-plugin-tpn.git"


def test_install_target_accepts_explicit_url():
    target = resolve_plugin_install_target(
        "git+https://github.com/tpn/sneeze-plugin-tpn.git"
    )

    assert target == "git+https://github.com/tpn/sneeze-plugin-tpn.git"


def test_scaffold_plugin_writes_expected_files(tmp_path):
    path = scaffold_plugin(
        "tpn",
        output_dir=tmp_path / "sneeze-plugin-tpn",
        init_git=False,
    )
    pyproject = tomllib.loads(
        (path / "pyproject.toml").read_text(encoding="utf-8")
    )
    commands_py = (path / "src/sneeze/tpn/commands.py").read_text(
        encoding="utf-8"
    )

    assert (path / "README.md").exists()
    assert (path / "AGENTS.md").exists()
    assert (path / "pyproject.toml").exists()
    assert not (path / "src/sneeze/__init__.py").exists()
    assert (path / "src/sneeze/tpn/commands.py").exists()
    assert "entry-points" not in pyproject["project"]
    assert pyproject["tool"]["setuptools"]["packages"]["find"]["include"] == [
        "sneeze",
        "sneeze.*",
    ]
    assert "class TpnPluginInfo" in commands_py
    assert "../sneeze/agents/PLUGINS.md" in (path / "AGENTS.md").read_text(
        encoding="utf-8"
    )


def test_scaffold_plugin_formats_underscore_username(tmp_path):
    path = scaffold_plugin(
        "trent_nelson",
        output_dir=tmp_path / "sneeze-plugin-trent-nelson",
        init_git=False,
    )
    pyproject = tomllib.loads(
        (path / "pyproject.toml").read_text(encoding="utf-8")
    )
    commands_py = (path / "src/sneeze/trent_nelson/commands.py").read_text(
        encoding="utf-8"
    )

    assert "entry-points" not in pyproject["project"]
    assert "class TrentNelsonPluginInfo" in commands_py


def test_scaffolded_plugin_is_discovered_by_namespace(tmp_path):
    path = scaffold_plugin(
        "tpn",
        output_dir=tmp_path / "sneeze-plugin-tpn",
        init_git=False,
    )
    core_src = Path(sneeze.__file__).resolve().parents[1]
    pythonpath = [str(core_src), str(path / "src")]
    if os.environ.get("PYTHONPATH"):
        pythonpath.append(os.environ["PYTHONPATH"])
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(pythonpath)}

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json;"
                "from sneeze.plugin import discover_plugins;"
                "print(json.dumps([p.__dict__ for p in discover_plugins()]))"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert {
        "username": "tpn",
        "package": "sneeze.tpn",
        "source": "namespace",
    } in json.loads(result.stdout)


def test_import_plugin_modules_falls_back_when_config_module_absent(
    tmp_path,
    monkeypatch,
):
    package_dir = tmp_path / "sample_plugin"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "commands.py").write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("sample_plugin", None)
    sys.modules.pop("sample_plugin.commands", None)
    sys.modules.pop("sample_plugin.config", None)

    commands_module, config_module = import_plugin_modules(
        PluginSpec("sample", "sample_plugin", "test")
    )

    assert commands_module.__name__ == "sample_plugin.commands"
    assert config_module.__name__ == "sneeze.config"


def test_import_plugin_modules_preserves_config_dependency_errors(
    tmp_path,
    monkeypatch,
):
    package_dir = tmp_path / "broken_plugin"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "commands.py").write_text("", encoding="utf-8")
    (package_dir / "config.py").write_text(
        "import missing_plugin_dependency_for_test\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("broken_plugin", None)
    sys.modules.pop("broken_plugin.commands", None)
    sys.modules.pop("broken_plugin.config", None)

    with pytest.raises(ModuleNotFoundError) as exc_info:
        import_plugin_modules(PluginSpec("broken", "broken_plugin", "test"))

    assert exc_info.value.name == "missing_plugin_dependency_for_test"


def test_pip_helpers_use_current_interpreter(monkeypatch):
    calls = []

    def fake_run(args, check=False):
        calls.append((args, check))
        return object()

    monkeypatch.setattr("sneeze.plugin.subprocess.run", fake_run)

    pip_install_plugin("/tmp/plugin")
    pip_uninstall_plugin("tpn")

    assert calls[0][0][0] == calls[1][0][0]
    assert calls[0][0][1:4] == ["-m", "pip", "install"]
    assert calls[1][0][1:5] == ["-m", "pip", "uninstall", "-y"]
