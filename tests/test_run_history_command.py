import datetime as dt
import json

from sneeze import cli as sneeze_cli
from sneeze import runlog
from sneeze.runlog import SneezeCommandRunInstance


def test_run_history_filters_and_summarizes(tmp_path, monkeypatch, capsys):
    _use_run_dir(tmp_path, monkeypatch)
    _write_log(
        tmp_path,
        runlog.HOSTNAME,
        [
            _instance(
                "install-plugin",
                "2026-01-02T10:00:00+00:00",
                exit_code=0,
            ),
            _instance(
                "install-plugin",
                "2026-01-03T10:00:00+00:00",
                exit_code=1,
            ),
            _instance("remove-plugin", "2026-01-02T12:00:00+00:00"),
        ],
    )

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "run-history",
        "--command",
        "install-plugin",
        "--exit-code",
        "0",
        "--start-date",
        "2026-01-02",
        "--end-date",
        "2026-01-02",
        "--summary",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 0
    assert captured.out.splitlines() == [
        "runs: 1",
        "install-plugin: 1",
    ]


def test_run_history_friendly_timestamps_all_hosts(
    tmp_path,
    monkeypatch,
    capsys,
):
    _use_run_dir(tmp_path, monkeypatch)
    _write_log(
        tmp_path,
        runlog.HOSTNAME,
        [_instance("install-plugin", "2026-01-02T10:00:00+00:00")],
    )
    _write_log(
        tmp_path,
        "other",
        [
            _instance(
                "remove-plugin",
                "2026-01-02T11:00:00+00:00",
                host="other",
            )
        ],
    )

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "run-history",
        "--all-hosts",
        "--friendly-timestamps",
        "--oldest-first",
        auto_plugins=False,
    )
    captured = capsys.readouterr()
    install_stamp = dt.datetime.fromisoformat(
        "2026-01-02T10:00:00+00:00"
    ).astimezone()
    remove_stamp = dt.datetime.fromisoformat(
        "2026-01-02T11:00:00+00:00"
    ).astimezone()

    assert cli.returncode == 0
    assert f"{runlog.HOSTNAME}:" in captured.out
    assert "other:" in captured.out
    assert (
        f"    [{install_stamp:%Y-%m-%d %H:%M:%S}] sne install-plugin"
        in captured.out
    )
    assert (
        f"    [{remove_stamp:%Y-%m-%d %H:%M:%S}] sne remove-plugin"
        in captured.out
    )


def test_run_history_filters_argv_and_git_rev(
    tmp_path,
    monkeypatch,
    capsys,
):
    _use_run_dir(tmp_path, monkeypatch)
    _write_log(
        tmp_path,
        runlog.HOSTNAME,
        [
            _instance(
                "install-plugin",
                "2026-01-02T10:00:00+00:00",
                argv=["sne", "install-plugin", "tpn"],
                git_rev="abcdef123",
            ),
            _instance(
                "install-plugin",
                "2026-01-02T11:00:00+00:00",
                argv=["sne", "install-plugin", "dave"],
                git_rev="999999",
            ),
        ],
    )

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "run-history",
        "--argv-contains",
        "tpn",
        "--git-rev",
        "abc",
        "--summary",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 0
    assert captured.out.splitlines() == [
        "runs: 1",
        "install-plugin: 1",
    ]


def test_run_history_rejects_end_date_before_start_date(
    tmp_path,
    monkeypatch,
    capsys,
):
    _use_run_dir(tmp_path, monkeypatch)

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "run-history",
        "--start-date",
        "2026-01-03",
        "--end-date",
        "2026-01-02",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 1
    assert "end date" in captured.err


def _use_run_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(runlog, "SNEEZE_RUN_DIR", str(tmp_path))


def _write_log(tmp_path, hostname, instances):
    path = tmp_path / f"sne-{hostname}.json"
    path.write_text(
        json.dumps([instance.dump() for instance in instances]),
        encoding="utf-8",
    )


def _instance(
    command,
    started_at,
    *,
    exit_code=0,
    host=None,
    argv=None,
    git_rev=None,
):
    started = dt.datetime.fromisoformat(started_at)
    return SneezeCommandRunInstance(
        argv=argv or ["sne", command],
        command=command,
        hostname=host or runlog.HOSTNAME,
        username="trent",
        started_at=started,
        ended_at=started,
        duration_s=0.1,
        exit_code=exit_code,
        git_rev=git_rev,
    )
