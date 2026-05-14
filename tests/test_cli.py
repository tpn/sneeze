import sys
from queue import Queue
from types import SimpleNamespace

import pytest

from sneeze import cli as sneeze_cli
from sneeze.command import Command
from sneeze.plugin import PluginSpec
from sneeze.util import Options


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

    for flag in ("--version", "-v", "-V"):
        cli = sneeze_cli.run(
            "sne",
            "sneeze",
            flag,
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


def test_interactive_unknown_command_returns_cli(
    monkeypatch, tmp_path, capsys
):
    _use_tmp_run_dir(tmp_path, monkeypatch)

    cli = sneeze_cli.run("sne sneeze nope")
    captured = capsys.readouterr()

    assert cli.returncode == 1
    assert "Unknown subcommand 'nope'" in captured.err


def test_cli_does_not_dispatch_arbitrary_instance_attributes(
    monkeypatch,
    tmp_path,
    capsys,
):
    _use_tmp_run_dir(tmp_path, monkeypatch)

    for name in ("modules", "run"):
        cli = sneeze_cli.run(
            "sne",
            "sneeze",
            name,
            auto_plugins=False,
        )
        captured = capsys.readouterr()

        assert cli.returncode == 1
        assert f"Unknown subcommand '{name}'" in captured.err


def test_queue_unknown_command_fails_without_attribute_error(
    monkeypatch,
    tmp_path,
    capsys,
):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    queue = Queue()
    queue.put(["nope"])
    cli = sneeze_cli.CLI(
        program_name="sne",
        module_names=["sneeze"],
        args_queue=queue,
        auto_plugins=False,
    )

    cli.run()
    captured = capsys.readouterr()

    assert cli.returncode == 1
    assert "Unknown subcommand 'nope'" in captured.err


def test_queue_marks_task_done_when_command_raises(monkeypatch, tmp_path):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    queue = Queue()
    queue.put(
        [
            "run-history",
            "--start-date",
            "2026-01-03",
            "--end-date",
            "2026-01-02",
        ]
    )
    cli = sneeze_cli.CLI(
        program_name="sne",
        module_names=["sneeze"],
        args_queue=queue,
        auto_plugins=False,
    )

    with pytest.raises(Exception, match="end date"):
        cli.run()

    assert queue.unfinished_tasks == 0


def test_queue_marks_pending_tasks_done_when_command_raises(
    monkeypatch,
    tmp_path,
):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    queue = Queue()
    queue.put(
        [
            "run-history",
            "--start-date",
            "2026-01-03",
            "--end-date",
            "2026-01-02",
        ]
    )
    queue.put(["run-history"])
    cli = sneeze_cli.CLI(
        program_name="sne",
        module_names=["sneeze"],
        args_queue=queue,
        auto_plugins=False,
    )

    with pytest.raises(Exception, match="end date"):
        cli.run()

    assert queue.unfinished_tasks == 0


def test_reused_command_instances_do_not_replay_exit_functions():
    calls = []

    class ExitCommand(Command):
        def run(self):
            self.on_exit(calls.append, "called")

    command = ExitCommand()
    command.options = Options()

    command.start()
    command.start()

    assert calls == ["called", "called"]


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


def test_init_plugin_cli_happy_path(tmp_path, monkeypatch, capsys):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    plugin_dir = tmp_path / "sneeze-plugin-acme"

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "init-plugin",
        "acme",
        "--output-dir",
        str(plugin_dir),
        "--no-git",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 0
    assert "initialized plugin at" in captured.out
    assert (plugin_dir / "pyproject.toml").exists()
    assert not (plugin_dir / ".git").exists()


def test_init_plugin_cli_formats_errors(tmp_path, monkeypatch, capsys):
    _use_tmp_run_dir(tmp_path, monkeypatch)

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "init-plugin",
        "bad!",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 1
    assert "sne init-plugin failed:" in captured.err


def test_install_plugin_cli_happy_path(tmp_path, monkeypatch, capsys):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    local = tmp_path / "sneeze-plugin-tpn"
    local.mkdir()
    calls = []

    def fake_install(target, editable=True):
        calls.append((target, editable))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("sneeze.commands.pip_install_plugin", fake_install)

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "install-plugin",
        "tpn",
        "--src-dir",
        str(tmp_path),
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 0
    assert calls == [(str(local), True)]
    assert "installed editable plugin" in captured.out


def test_install_plugin_cli_reports_pip_failure(
    tmp_path,
    monkeypatch,
    capsys,
):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "sneeze.commands.pip_install_plugin",
        lambda target, editable=True: SimpleNamespace(returncode=7),
    )

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "install-plugin",
        "tpn",
        "--src-dir",
        str(tmp_path),
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 1
    assert "pip install failed with exit 7" in captured.err


def test_remove_plugin_cli_happy_path(tmp_path, monkeypatch, capsys):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    calls = []

    def fake_uninstall(name):
        calls.append(name)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(
        "sneeze.commands.pip_uninstall_plugin", fake_uninstall
    )

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "remove-plugin",
        "tpn",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 0
    assert calls == ["tpn"]
    assert "removed plugin tpn" in captured.out


def test_remove_plugin_cli_reports_pip_failure(
    tmp_path,
    monkeypatch,
    capsys,
):
    _use_tmp_run_dir(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "sneeze.commands.pip_uninstall_plugin",
        lambda name: SimpleNamespace(returncode=9),
    )

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "remove-plugin",
        "tpn",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 1
    assert "pip uninstall failed with exit 9" in captured.err


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


def _use_tmp_run_dir(tmp_path, monkeypatch):
    from sneeze import runlog

    monkeypatch.setattr(runlog, "SNEEZE_RUN_DIR", str(tmp_path / "run"))
