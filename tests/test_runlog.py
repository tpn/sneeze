import datetime as dt
import json
import os
import threading
import time

import pytest

from sneeze.runlog import (
    RunLogCorruptionError,
    RunLogError,
    SneezeCommandRunInstance,
    append_run_instance,
    load_run_instances,
    repair_run_log,
)


def _make_instance(command):
    now = dt.datetime.now(dt.UTC)
    return SneezeCommandRunInstance(
        argv=["sne", command],
        command=command,
        hostname="air",
        username="trent",
        started_at=now,
        ended_at=now,
        duration_s=0.1,
        exit_code=0,
    )


def test_load_run_instances_reads_valid_json_array(tmp_path):
    path = tmp_path / "sne-air.json"
    instances = [
        _make_instance("install-plugin"),
        _make_instance("run-history"),
    ]
    payload = "[" + ",".join(instance.dump_json() for instance in instances)
    payload += "]"
    path.write_text(payload, encoding="utf-8")

    loaded = load_run_instances([str(path)])

    assert [item.command for item in loaded] == [
        "install-plugin",
        "run-history",
    ]


def test_load_run_instances_raises_for_fragmented_json_log(tmp_path):
    path = tmp_path / "sne-air.json"
    first = _make_instance("install-plugin").dump_json()
    second = _make_instance("run-history").dump_json()
    path.write_text(f"[\n{first}\n]\n,\n{second}\n]\n", encoding="utf-8")

    with pytest.raises(RunLogCorruptionError) as excinfo:
        load_run_instances([str(path)])

    assert str(path) in str(excinfo.value)
    assert "recoverable trailing items" in str(excinfo.value)


def test_load_run_instances_can_recover_fragmented_json_log(tmp_path):
    path = tmp_path / "sne-air.json"
    first = _make_instance("install-plugin").dump_json()
    second = _make_instance("run-history").dump_json()
    path.write_text(f"[\n{first}\n]\n,\n{second}\n]\n", encoding="utf-8")

    loaded = load_run_instances([str(path)], strict=False)

    assert [item.command for item in loaded] == [
        "install-plugin",
        "run-history",
    ]


def test_append_run_instance_refuses_corrupted_log(tmp_path):
    path = tmp_path / "sne-air.json"
    path.write_text('[{"argv":["sne","run-history"]}] garbage')

    with pytest.raises(RunLogCorruptionError):
        append_run_instance(
            _make_instance("install-plugin"),
            hostname="air",
            run_dir=str(tmp_path),
        )

    assert path.read_text(encoding="utf-8") == (
        '[{"argv":["sne","run-history"]}] garbage'
    )


def test_repair_run_log_rewrites_recoverable_fragments(tmp_path):
    path = tmp_path / "sne-air.json"
    first = _make_instance("install-plugin").dump_json()
    second = _make_instance("run-history").dump_json()
    path.write_text(f"[\n{first}\n]\n,\n{second}\n]\n", encoding="utf-8")

    repaired = repair_run_log(str(path))

    assert repaired == 2
    loaded = load_run_instances([str(path)])
    assert [item.command for item in loaded] == [
        "install-plugin",
        "run-history",
    ]


def test_append_run_instance_removes_stale_lock(tmp_path):
    path = tmp_path / "sne-air.json"
    lock_path = str(path) + ".lock"
    with open(lock_path, "w", encoding="utf-8") as handle:
        json.dump({"pid": 999999, "hostname": "air"}, handle)

    append_run_instance(
        _make_instance("run-history"),
        hostname="air",
        run_dir=str(tmp_path),
    )

    loaded = load_run_instances([str(path)])
    assert [item.command for item in loaded] == ["run-history"]
    assert not os.path.exists(lock_path)


def test_append_run_instance_waits_for_live_lock_release(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    append_run_instance(
        _make_instance("install-plugin"),
        hostname="air",
        run_dir=str(tmp_path),
    )
    lock_path = str(path) + ".lock"
    with open(lock_path, "w", encoding="utf-8") as handle:
        json.dump({"pid": os.getpid(), "hostname": "air"}, handle)

    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)

    def release_lock():
        time.sleep(0.05)
        os.unlink(lock_path)

    releaser = threading.Thread(target=release_lock, daemon=True)
    releaser.start()
    append_run_instance(
        _make_instance("run-history"),
        hostname="air",
        run_dir=str(tmp_path),
    )
    releaser.join(timeout=1)

    loaded = load_run_instances([str(path)])
    assert [item.command for item in loaded] == [
        "install-plugin",
        "run-history",
    ]
    assert not os.path.exists(lock_path)


def test_append_run_instance_times_out_for_live_lock(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    lock_path = str(path) + ".lock"
    with open(lock_path, "w", encoding="utf-8") as handle:
        json.dump({"pid": os.getpid(), "hostname": "air"}, handle)

    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("sneeze.runlog._LOCK_TIMEOUT_S", 0.02)

    with pytest.raises(RunLogError, match="timed out acquiring run log lock"):
        append_run_instance(
            _make_instance("run-history"),
            hostname="air",
            run_dir=str(tmp_path),
        )


def test_default_repo_root_uses_current_working_directory(
    tmp_path,
    monkeypatch,
):
    from sneeze.runlog import CommandRunContext

    monkeypatch.chdir(tmp_path)

    ctx = CommandRunContext(["sne", "run-history"])

    assert ctx.repo_root == str(tmp_path)


def test_cli_run_history_fails_loudly_for_corrupted_log(
    tmp_path,
    monkeypatch,
    capsys,
):
    from sneeze import cli as sneeze_cli
    from sneeze import runlog

    path = tmp_path / f"sne-{runlog.HOSTNAME}.json"
    first = _make_instance("install-plugin").dump_json()
    second = _make_instance("run-history").dump_json()
    path.write_text(f"[\n{first}\n]\n,\n{second}\n]\n", encoding="utf-8")

    monkeypatch.setattr(runlog, "SNEEZE_RUN_DIR", str(tmp_path))

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "run-history",
        auto_plugins=False,
    )
    captured = capsys.readouterr()

    assert cli.returncode == 1
    assert "sne run-history failed:" in captured.err
    assert "recoverable trailing items" in captured.err
