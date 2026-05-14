import sys
from pathlib import Path

from sneeze import cli as sneeze_cli
from sneeze.plugin import PluginSpec


def test_default_program_name_matches_console_script():
    assert sneeze_cli.DEFAULT_PROGRAM_NAME == "sne"


def test_cli_introspection_loads_core_commands_without_output(capsys):
    cli = sneeze_cli.CLI(
        program_name="sne",
        module_names=["sneeze"],
        introspect=True,
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.commandline is None
    assert "run-history" in cli._commands_by_name
    assert "install-plugin" in cli._commands_by_name
    assert captured.out == ""
    assert captured.err == ""


def test_version_flags_exit_cleanly(capsys, monkeypatch, tmp_path):
    from sneeze import runlog

    monkeypatch.setattr(runlog, "SNEEZE_RUN_DIR", str(tmp_path))

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "--version",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 0
    assert captured.out == "0.1\n"
    assert "Unknown subcommand" not in captured.err


def test_interactive_run_does_not_poison_later_runs(
    monkeypatch,
    tmp_path,
    capsys,
):
    from sneeze import runlog

    monkeypatch.setattr(runlog, "SNEEZE_RUN_DIR", str(tmp_path))

    interactive_command = sneeze_cli.run("sne sneeze run-history")
    capsys.readouterr()

    assert interactive_command.interactive is True
    assert sneeze_cli.INTERACTIVE is False

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "run-history",
        auto_plugins=False,
    )

    assert cli.commandline.command.interactive is False


def test_cli_prefixes_duplicate_plugin_command_names(
    tmp_path,
    monkeypatch,
):
    _write_plugin(tmp_path, "alpha_plugin", "alpha")
    _write_plugin(tmp_path, "bravo_plugin", "bravo")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(
        sneeze_cli,
        "discover_plugins",
        lambda: [
            PluginSpec("alpha", "alpha_plugin", "test"),
            PluginSpec("bravo", "bravo_plugin", "test"),
        ],
    )

    cli = sneeze_cli.CLI(
        program_name="sne",
        module_names=["sneeze"],
        introspect=True,
    )

    assert "alpha-foo" in cli._commands_by_name
    assert "bravo-foo" in cli._commands_by_name
    assert "foo" not in cli._commands_by_name


def test_cli_keeps_core_name_and_prefixes_plugin_collision(
    tmp_path,
    monkeypatch,
):
    _write_plugin(tmp_path, "alpha_plugin", "alpha", "RunHistoryCommand")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setattr(
        sneeze_cli,
        "discover_plugins",
        lambda: [PluginSpec("alpha", "alpha_plugin", "test")],
    )

    cli = sneeze_cli.CLI(
        program_name="sne",
        module_names=["sneeze"],
        introspect=True,
    )

    assert "run-history" in cli._commands_by_name
    assert "alpha-run-history" in cli._commands_by_name


def _write_plugin(tmp_path, package, username, class_name="Foo"):
    for name in [package, f"{package}.commands", f"{package}.config"]:
        sys.modules.pop(name, None)
    package_dir = tmp_path / package
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("", encoding="utf-8")
    (package_dir / "config.py").write_text(
        "from sneeze.config import Config\n",
        encoding="utf-8",
    )
    (package_dir / "commands.py").write_text(
        f"""
from sneeze.commandinvariant import InvariantAwareCommand


class {class_name}(InvariantAwareCommand):
    _shortname_ = "{username[:1]}f"

    def run(self):
        self._out("{package}")
""".lstrip(),
        encoding="utf-8",
    )
    sys.path.insert(0, str(Path(tmp_path)))
