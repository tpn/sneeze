import datetime as dt
import fcntl
import json
import os
import threading
import time

import pytest

import sneeze.runlog as runlog_module
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


def _hold_run_log_lock(lock_path, payload=None):
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if payload is not None:
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps(payload).encode("utf-8"))
    return fd


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


def test_append_run_instance_recovers_corrupted_log(tmp_path):
    path = tmp_path / "sne-air.json"
    first = _make_instance("run-history").dump_json()
    path.write_text(f"[\n{first}\n] garbage", encoding="utf-8")

    append_run_instance(
        _make_instance("install-plugin"),
        hostname="air",
        run_dir=str(tmp_path),
    )

    loaded = load_run_instances([str(path)])
    assert [item.command for item in loaded] == [
        "run-history",
        "install-plugin",
    ]
    assert (tmp_path / "sne-air.json.corrupt").read_text(
        encoding="utf-8"
    ) == f"[\n{first}\n] garbage"
    assert (tmp_path / "sne-air.json.corrupt").stat().st_mode & 0o777 == 0o600


def test_append_run_instance_uses_next_corrupt_backup_name(tmp_path):
    path = tmp_path / "sne-air.json"
    first = _make_instance("run-history").dump_json()
    path.write_text(f"[\n{first}\n] garbage", encoding="utf-8")
    (tmp_path / "sne-air.json.corrupt").write_text(
        "older backup",
        encoding="utf-8",
    )

    append_run_instance(
        _make_instance("install-plugin"),
        hostname="air",
        run_dir=str(tmp_path),
    )

    assert (tmp_path / "sne-air.json.corrupt.1").read_text(
        encoding="utf-8"
    ) == f"[\n{first}\n] garbage"


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


def test_repair_run_log_missing_path_is_noop(tmp_path):
    path = tmp_path / "missing.json"

    repaired = repair_run_log(str(path))

    assert repaired == 0
    assert not path.exists()


def test_append_run_instance_waits_for_legacy_lock_removal(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    lock_path = str(path) + ".lock"
    with open(lock_path, "w", encoding="utf-8") as handle:
        json.dump(
            {"pid": 999999, "hostname": runlog_module.HOSTNAME},
            handle,
        )
    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)

    def release_legacy_lock():
        time.sleep(0.05)
        os.unlink(lock_path)

    releaser = threading.Thread(target=release_legacy_lock, daemon=True)
    releaser.start()

    append_run_instance(
        _make_instance("run-history"),
        hostname="air",
        run_dir=str(tmp_path),
    )
    releaser.join(timeout=1)

    loaded = load_run_instances([str(path)])
    assert [item.command for item in loaded] == ["run-history"]
    payload = json.loads(
        (tmp_path / "sne-air.json.lock.v2").read_text(encoding="utf-8")
    )
    assert payload["lock_version"] == 2
    assert payload["host_fqdn"] == runlog_module.HOSTFQDN
    assert payload["host_id"] == runlog_module._run_log_host_id()
    assert payload["owner_token"]
    assert not (tmp_path / "sne-air.json.lock").exists()


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
    held_lock = runlog_module._acquire_run_log_lock(str(path))

    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)

    def release_lock():
        time.sleep(0.05)
        runlog_module._release_run_log_lock(held_lock)

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
    assert os.path.exists(str(path) + ".lock.v2")
    assert not os.path.exists(str(path) + ".lock")


def test_append_run_instance_times_out_for_live_lock(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    held_lock = runlog_module._acquire_run_log_lock(str(path))

    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("sneeze.runlog._LOCK_TIMEOUT_S", 0.02)

    try:
        with pytest.raises(
            RunLogError, match="timed out acquiring run log lock"
        ):
            append_run_instance(
                _make_instance("run-history"),
                hostname="air",
                run_dir=str(tmp_path),
            )
    finally:
        runlog_module._release_run_log_lock(held_lock)


def test_run_log_lock_uses_kernel_ownership_not_payload_identity(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    lock_path = str(path) + ".lock.v2"
    lock_fd = _hold_run_log_lock(
        lock_path,
        {
            "lock_version": 2,
            "pid": 999999,
            "hostname": runlog_module.HOSTNAME,
            "host_fqdn": runlog_module.HOSTFQDN,
            "host_id": "different-host-id",
            "owner_token": "other-owner",
        },
    )
    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("sneeze.runlog._LOCK_TIMEOUT_S", 0.02)

    try:
        with pytest.raises(
            RunLogError, match="timed out acquiring run log lock"
        ):
            append_run_instance(
                _make_instance("run-history"),
                hostname="air",
                run_dir=str(tmp_path),
            )
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def test_run_log_lock_is_released_when_owner_process_exits(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    first_lock = runlog_module._acquire_run_log_lock(str(path))
    os.close(first_lock.fd)
    os.close(first_lock.compatibility_fd)
    monkeypatch.setattr(runlog_module, "_pid_is_alive", lambda _pid: False)

    second_lock = runlog_module._acquire_run_log_lock(str(path))
    try:
        assert second_lock.owner_token != first_lock.owner_token
    finally:
        runlog_module._release_run_log_lock(second_lock)


def test_young_legacy_run_log_lock_fails_closed_with_remediation(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    lock_path.write_text(
        json.dumps({"pid": 999999, "hostname": runlog_module.HOSTNAME}),
        encoding="utf-8",
    )
    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("sneeze.runlog._LOCK_TIMEOUT_S", 0.02)

    with pytest.raises(
        RunLogError, match="verify that no other Sneeze process is writing"
    ):
        runlog_module._acquire_run_log_lock(str(path))


def test_stale_local_legacy_run_log_lock_is_reclaimed(tmp_path, monkeypatch):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    lock_path.write_text(
        json.dumps({"pid": 999999, "hostname": runlog_module.HOSTNAME}),
        encoding="utf-8",
    )
    old_time = time.time() - 61
    os.utime(lock_path, (old_time, old_time))
    monkeypatch.setattr("sneeze.runlog._LEGACY_LOCK_STALE_S", 60)
    monkeypatch.setattr(runlog_module, "_pid_is_alive", lambda _pid: False)

    lock = runlog_module._acquire_run_log_lock(str(path))
    runlog_module._release_run_log_lock(lock)

    assert not lock_path.exists()


def test_stale_remote_legacy_run_log_lock_fails_closed(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    lock_path.write_text(
        json.dumps({"pid": 999999, "hostname": "remote-host"}),
        encoding="utf-8",
    )
    old_time = time.time() - 61
    os.utime(lock_path, (old_time, old_time))
    monkeypatch.setattr("sneeze.runlog._LEGACY_LOCK_STALE_S", 60)
    monkeypatch.setattr("sneeze.runlog._LOCK_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr("sneeze.runlog._LOCK_TIMEOUT_S", 0.02)

    def fail_pid_probe(_pid):
        raise AssertionError("remote PID must not be checked locally")

    monkeypatch.setattr(runlog_module, "_pid_is_alive", fail_pid_probe)

    with pytest.raises(
        RunLogError, match="verify that no other Sneeze process is writing"
    ):
        runlog_module._acquire_run_log_lock(str(path))


def test_run_log_lock_rejects_symlink_without_modifying_target(tmp_path):
    path = tmp_path / "sne-air.json"
    target = tmp_path / "target.txt"
    target.write_text("do not replace", encoding="utf-8")
    (tmp_path / "sne-air.json.lock.v2").symlink_to(target)

    with pytest.raises(RunLogError, match="failed to open run log lock"):
        runlog_module._acquire_run_log_lock(str(path))

    assert target.read_text(encoding="utf-8") == "do not replace"


def test_run_log_lock_rejects_hard_link_without_modifying_target(tmp_path):
    path = tmp_path / "sne-air.json"
    target = tmp_path / "target.txt"
    target.write_text("do not replace", encoding="utf-8")
    os.link(target, tmp_path / "sne-air.json.lock.v2")

    with pytest.raises(RunLogError, match="multiple hard links"):
        runlog_module._acquire_run_log_lock(str(path))

    assert target.read_text(encoding="utf-8") == "do not replace"


@pytest.mark.parametrize("payload", (b"", b"{not-json"))
def test_run_log_lock_recovers_incomplete_v2_metadata(tmp_path, payload):
    path = tmp_path / "sne-air.json"
    flock_path = tmp_path / "sne-air.json.lock.v2"
    flock_path.write_bytes(payload)

    lock = runlog_module._acquire_run_log_lock(str(path))
    try:
        metadata = json.loads(flock_path.read_text(encoding="utf-8"))
        assert metadata["lock_version"] == 2
        assert metadata["owner_token"] == lock.owner_token
    finally:
        runlog_module._release_run_log_lock(lock)


def test_run_log_lock_retries_unlinked_descriptor(tmp_path):
    flock_path = tmp_path / "sne-air.json.lock.v2"
    old_fd = os.open(flock_path, os.O_CREAT | os.O_RDWR, 0o600)
    os.unlink(flock_path)
    new_fd = os.open(flock_path, os.O_CREAT | os.O_RDWR, 0o600)

    try:
        assert not runlog_module._validate_run_log_lock_file(
            str(flock_path), old_fd
        )
    finally:
        os.close(old_fd)
        os.close(new_fd)


def test_run_log_compatibility_lock_exists_only_while_held(tmp_path):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"

    lock = runlog_module._acquire_run_log_lock(str(path))
    assert lock_path.exists()
    runlog_module._release_run_log_lock(lock)

    assert not lock_path.exists()
    assert (tmp_path / "sne-air.json.lock.v2").exists()


def test_run_log_lock_recovers_interrupted_compatibility_link(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    old_token = "interrupted-owner"
    candidate_path = tmp_path / f"sne-air.json.lock.{old_token}.tmp"
    candidate_path.write_text(
        runlog_module._current_lock_payload(old_token),
        encoding="utf-8",
    )
    os.link(candidate_path, lock_path)
    monkeypatch.setattr(runlog_module, "_pid_is_alive", lambda _pid: False)

    lock = runlog_module._acquire_run_log_lock(str(path))
    try:
        assert lock.owner_token != old_token
        assert not candidate_path.exists()
    finally:
        runlog_module._release_run_log_lock(lock)

    assert not lock_path.exists()


def test_live_compatibility_publication_window_waits(tmp_path, monkeypatch):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    owner_token = "live-owner"
    candidate_path = tmp_path / f"sne-air.json.lock.{owner_token}.tmp"
    candidate_path.write_text(
        runlog_module._current_lock_payload(owner_token),
        encoding="utf-8",
    )
    os.link(candidate_path, lock_path)
    monkeypatch.setattr(runlog_module, "_LOCK_POLL_INTERVAL_S", 0.01)

    def finish_publication_and_release():
        time.sleep(0.05)
        os.unlink(candidate_path)
        os.unlink(lock_path)

    releaser = threading.Thread(
        target=finish_publication_and_release,
        daemon=True,
    )
    releaser.start()
    lock = runlog_module._acquire_run_log_lock(str(path))
    runlog_module._release_run_log_lock(lock)
    releaser.join(timeout=1)

    assert not lock_path.exists()


def test_remote_v2_compatibility_lock_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    payload = json.loads(runlog_module._current_lock_payload("remote-owner"))
    payload["host_id"] = "remote-host-id"
    lock_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(runlog_module, "_LOCK_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(runlog_module, "_LOCK_TIMEOUT_S", 0.02)

    with pytest.raises(
        RunLogError, match="verify that no other Sneeze process is writing"
    ):
        runlog_module._acquire_run_log_lock(str(path))

    assert lock_path.exists()


def test_live_v2_owner_survives_flock_path_replacement(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    flock_path = tmp_path / "sne-air.json.lock.v2"
    compatibility_path = tmp_path / "sne-air.json.lock"
    first_lock = runlog_module._acquire_run_log_lock(str(path))
    os.unlink(flock_path)
    monkeypatch.setattr(runlog_module, "_LOCK_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(runlog_module, "_LOCK_TIMEOUT_S", 0.02)

    try:
        with pytest.raises(
            RunLogError,
            match="verify that no other Sneeze process is writing",
        ):
            runlog_module._acquire_run_log_lock(str(path))
        assert compatibility_path.exists()
    finally:
        runlog_module._release_run_log_lock(first_lock)


def test_run_log_lock_retries_flock_reopen_race(tmp_path, monkeypatch):
    lock_path = tmp_path / "sne-air.json.lock.v2"
    calls = 0

    def racing_open(path, flags, mode=0o777):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FileExistsError
        raise FileNotFoundError

    monkeypatch.setattr(runlog_module.os, "open", racing_open)

    assert runlog_module._open_run_log_lock(str(lock_path)) is None
    assert calls == 2


def test_run_log_lock_retries_compatibility_reopen_race(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    real_link = os.link
    calls = 0

    def racing_link(source, target, *, follow_symlinks=True):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FileExistsError
        return real_link(
            source,
            target,
            follow_symlinks=follow_symlinks,
        )

    monkeypatch.setattr(runlog_module.os, "link", racing_link)

    lock = runlog_module._acquire_run_log_lock(str(path))
    runlog_module._release_run_log_lock(lock)

    assert calls == 2


def test_run_log_compatibility_lock_rejects_fifo_without_blocking(tmp_path):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    os.mkfifo(lock_path)

    with pytest.raises(RunLogError, match="not a regular file"):
        runlog_module._acquire_run_log_lock(str(path))


def test_run_log_timeout_closes_descriptor_once(tmp_path, monkeypatch):
    path = tmp_path / "sne-air.json"
    fd = os.open(tmp_path / "owned-lock", os.O_CREAT | os.O_RDWR, 0o600)
    real_close = os.close
    close_count = 0

    def recording_close(candidate_fd):
        nonlocal close_count
        if candidate_fd == fd:
            close_count += 1
        real_close(candidate_fd)

    monkeypatch.setattr(runlog_module, "_LOCK_TIMEOUT_S", 0)
    monkeypatch.setattr(runlog_module, "_open_run_log_lock", lambda _path: fd)
    monkeypatch.setattr(
        runlog_module,
        "_validate_run_log_lock_file",
        lambda _path, _fd: False,
    )
    monkeypatch.setattr(runlog_module.os, "close", recording_close)

    with pytest.raises(RunLogError, match="timed out acquiring run log lock"):
        runlog_module._acquire_run_log_lock(str(path))

    assert close_count == 1


def test_compatibility_link_cleanup_removes_installed_lock(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "sne-air.json"
    lock_path = tmp_path / "sne-air.json.lock"
    real_unlink = os.unlink
    failed = False

    def fail_first_candidate_unlink(candidate):
        nonlocal failed
        if not failed and str(candidate).endswith(".tmp"):
            failed = True
            raise PermissionError("injected candidate unlink failure")
        return real_unlink(candidate)

    monkeypatch.setattr(
        runlog_module.os, "unlink", fail_first_candidate_unlink
    )

    with pytest.raises(PermissionError, match="injected candidate unlink"):
        runlog_module._acquire_run_log_lock(str(path))

    assert failed
    assert not lock_path.exists()
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize(
    "value",
    ("not-a-number", "nan", "inf", "-inf", "-1"),
)
def test_invalid_run_log_lock_timeout_env_uses_default(monkeypatch, value):
    monkeypatch.setenv("SNEEZE_TEST_LOCK_TIMEOUT", value)

    assert runlog_module._float_env("SNEEZE_TEST_LOCK_TIMEOUT", 30) == 30.0


def test_valid_run_log_lock_timeout_env_is_used(monkeypatch):
    monkeypatch.setenv("SNEEZE_TEST_LOCK_TIMEOUT", "1.25")

    assert runlog_module._float_env("SNEEZE_TEST_LOCK_TIMEOUT", 30) == 1.25


def test_default_repo_root_is_none_outside_git_repo(
    tmp_path,
    monkeypatch,
):
    from sneeze.runlog import CommandRunContext

    monkeypatch.chdir(tmp_path)

    ctx = CommandRunContext(["sne", "run-history"])

    assert ctx.repo_root is None


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


def test_cli_dispatch_appends_run_log_entry(tmp_path, monkeypatch, capsys):
    from sneeze import cli as sneeze_cli
    from sneeze import runlog

    monkeypatch.setattr(runlog, "SNEEZE_RUN_DIR", str(tmp_path))

    cli = sneeze_cli.run(
        "sne",
        "sneeze",
        "--version",
        auto_plugins=False,
    )
    capsys.readouterr()

    instances = load_run_instances(
        [runlog.get_run_log_path(run_dir=str(tmp_path))]
    )

    assert cli.returncode == 0
    assert len(instances) == 1
    assert instances[0].argv[-1] == "--version"
    assert instances[0].command == "--version"
    assert instances[0].exit_code == 0
