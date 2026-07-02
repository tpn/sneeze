import datetime as dt
import errno
import fcntl
import getpass
import json
import math
import os
import shutil
import stat
import tempfile
import time
import uuid
from dataclasses import dataclass
from functools import lru_cache

from .config import HOSTFQDN, HOSTNAME, SNEEZE_RUN_DIR
from .path import find_repo_root, join_path


def _float_env(name, default):
    try:
        value = float(os.environ.get(name, str(default)))
    except ValueError:
        return float(default)
    if not math.isfinite(value) or value < 0:
        return float(default)
    return value


RUN_LOG_PREFIX = "sne-"
RUN_LOG_SUFFIX = ".json"
RUN_LOG_LOCK_SUFFIX = ".lock"
_RUN_LOG_FLOCK_SUFFIX = ".lock.v2"
_LOCK_POLL_INTERVAL_S = 0.05
_LOCK_TIMEOUT_S = _float_env("SNEEZE_RUN_LOG_LOCK_TIMEOUT", 30)
_LEGACY_LOCK_STALE_S = _float_env(
    "SNEEZE_LEGACY_RUN_LOG_LOCK_STALE",
    300,
)


class RunLogError(Exception):
    pass


class RunLogCorruptionError(RunLogError):
    def __init__(self, path, message):
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}")


@dataclass
class _RunLogLock:
    path: str
    fd: int
    owner_token: str
    compatibility_path: str
    compatibility_fd: int


@dataclass
class SneezeCommandRunResources:
    user_cpu_s: float | None = None
    system_cpu_s: float | None = None
    max_rss_kb: int | None = None
    input_blocks: int | None = None
    output_blocks: int | None = None
    vol_ctx_switches: int | None = None
    invol_ctx_switches: int | None = None

    def dump(self):
        return {
            key: value
            for key, value in self.__dict__.items()
            if value is not None
        }


@dataclass
class SneezeCommandRunInstance:
    argv: list[str]
    hostname: str
    username: str
    started_at: dt.datetime
    ended_at: dt.datetime
    duration_s: float
    exit_code: int
    run_id: str | None = None
    command: str | None = None
    host_fqdn: str | None = None
    pid: int | None = None
    cwd: str | None = None
    repo_root: str | None = None
    git_rev: str | None = None
    resources: SneezeCommandRunResources | None = None
    error_type: str | None = None
    error_message: str | None = None

    def __post_init__(self):
        if self.run_id is None:
            self.run_id = str(uuid.uuid4())

    def dump(self):
        data = {
            "run_id": self.run_id,
            "argv": self.argv,
            "command": self.command,
            "hostname": self.hostname,
            "host_fqdn": self.host_fqdn,
            "username": self.username,
            "pid": self.pid,
            "cwd": self.cwd,
            "repo_root": self.repo_root,
            "git_rev": self.git_rev,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_s": self.duration_s,
            "exit_code": self.exit_code,
            "resources": self.resources.dump() if self.resources else None,
            "error_type": self.error_type,
            "error_message": self.error_message,
        }
        return {
            key: value for key, value in data.items() if value is not None
        }

    def dump_json(self):
        return json.dumps(self.dump(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def model_validate(cls, value):
        if not isinstance(value, dict):
            raise TypeError("run log entry must be an object")
        data = dict(value)
        resources = data.get("resources")
        if resources is not None:
            if not isinstance(resources, dict):
                raise TypeError("resources must be an object")
            data["resources"] = SneezeCommandRunResources(**resources)
        for key in ("started_at", "ended_at"):
            item = data.get(key)
            if isinstance(item, str):
                data[key] = dt.datetime.fromisoformat(item)
        return cls(**data)


def _get_rusage():
    try:
        import resource
    except ImportError:
        return None
    return resource.getrusage(resource.RUSAGE_SELF)


def _resource_delta(start, end):
    if not end:
        return None
    if not start:
        start = end
    return SneezeCommandRunResources(
        user_cpu_s=max(0.0, end.ru_utime - start.ru_utime),
        system_cpu_s=max(0.0, end.ru_stime - start.ru_stime),
        max_rss_kb=end.ru_maxrss,
        input_blocks=max(0, end.ru_inblock - start.ru_inblock),
        output_blocks=max(0, end.ru_oublock - start.ru_oublock),
        vol_ctx_switches=max(0, end.ru_nvcsw - start.ru_nvcsw),
        invol_ctx_switches=max(0, end.ru_nivcsw - start.ru_nivcsw),
    )


def _format_json_decode_error(err):
    return f"{err.msg} at line {err.lineno}, column {err.colno}"


def _count_recovered_items(values):
    total = 0
    for value in values:
        if isinstance(value, list):
            total += len(value)
        elif isinstance(value, dict):
            total += 1
    return total


def _run_log_lock_path(path):
    return f"{path}{RUN_LOG_LOCK_SUFFIX}"


def _run_log_flock_path(path):
    return f"{path}{_RUN_LOG_FLOCK_SUFFIX}"


@lru_cache(maxsize=1)
def _run_log_host_id():
    machine_id = None
    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            with open(path, encoding="utf-8") as handle:
                machine_id = handle.read().strip()
        except OSError:
            continue
        if machine_id:
            break
    return f"{HOSTFQDN}:{machine_id}" if machine_id else HOSTFQDN


def _current_lock_payload(owner_token):
    return json.dumps(
        {
            "lock_version": 2,
            "pid": os.getpid(),
            "hostname": HOSTNAME,
            "host_fqdn": HOSTFQDN,
            "host_id": _run_log_host_id(),
            "owner_token": owner_token,
            "acquired_at": dt.datetime.now(dt.UTC).isoformat(),
        }
    )


def _open_run_log_lock(lock_path):
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise RunLogError(
            "secure run log locking requires O_NOFOLLOW support"
        )
    flags = os.O_RDWR | nofollow
    try:
        return os.open(lock_path, flags | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        try:
            return os.open(lock_path, flags)
        except FileNotFoundError:
            return None
        except OSError as err:
            raise RunLogError(
                f"failed to open run log lock: {lock_path}: {err}"
            ) from err
    except OSError as err:
        raise RunLogError(
            f"failed to create run log lock: {lock_path}: {err}"
        ) from err


def _validate_run_log_lock_file(lock_path, fd):
    try:
        fd_stat = os.fstat(fd)
    except OSError as err:
        raise RunLogError(
            f"failed to inspect run log lock descriptor: {lock_path}: {err}"
        ) from err
    try:
        path_stat = os.stat(lock_path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as err:
        raise RunLogError(
            f"failed to inspect run log lock: {lock_path}: {err}"
        ) from err
    expected_uid = os.geteuid()
    if not stat.S_ISREG(fd_stat.st_mode):
        raise RunLogError(f"run log lock is not a regular file: {lock_path}")
    if fd_stat.st_uid != expected_uid:
        raise RunLogError(
            f"run log lock is not owned by uid {expected_uid}: {lock_path}"
        )
    if fd_stat.st_nlink == 0:
        return False
    if fd_stat.st_nlink > 1:
        raise RunLogError(
            f"run log lock has multiple hard links: {lock_path}"
        )
    return (fd_stat.st_dev, fd_stat.st_ino) == (
        path_stat.st_dev,
        path_stat.st_ino,
    )


def _read_run_log_lock_payload(fd):
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        raw = os.read(fd, 65537)
        if len(raw) > 65536:
            return None
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _is_current_run_log_lock_payload(payload):
    return bool(
        isinstance(payload, dict)
        and payload.get("lock_version") == 2
        and isinstance(payload.get("host_id"), str)
        and payload["host_id"]
        and isinstance(payload.get("owner_token"), str)
        and payload["owner_token"]
    )


def _pid_is_alive(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        return True
    return True


def _legacy_compatibility_lock_is_reclaimable(payload, fd):
    if (
        not isinstance(payload, dict)
        or payload.get("lock_version") is not None
    ):
        return False
    pid = payload.get("pid")
    if (
        payload.get("hostname") != HOSTNAME
        or not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or _pid_is_alive(pid)
    ):
        return False
    try:
        age = time.time() - os.fstat(fd).st_mtime
    except OSError:
        return False
    return age >= _LEGACY_LOCK_STALE_S


def _write_run_log_lock_payload(fd, owner_token):
    os.fchmod(fd, 0o600)
    os.ftruncate(fd, 0)
    os.lseek(fd, 0, os.SEEK_SET)
    payload = _current_lock_payload(owner_token).encode("utf-8")
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("failed to write run log lock payload")
        view = view[written:]
    os.fsync(fd)


def _unlink_run_log_lock_if_matches(lock_path, fd):
    try:
        fd_stat = os.fstat(fd)
        path_stat = os.stat(lock_path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    if (fd_stat.st_dev, fd_stat.st_ino) != (
        path_stat.st_dev,
        path_stat.st_ino,
    ):
        return False
    os.unlink(lock_path)
    return True


def _compatibility_lock_is_reclaimable(lock_path, fd):
    try:
        fd_stat = os.fstat(fd)
        path_stat = os.stat(lock_path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    expected_uid = os.geteuid()
    if not stat.S_ISREG(fd_stat.st_mode):
        raise RunLogError(
            f"run log compatibility lock is not a regular file: {lock_path}"
        )
    if fd_stat.st_uid != expected_uid:
        raise RunLogError(
            "run log compatibility lock is not owned by "
            f"uid {expected_uid}: {lock_path}"
        )
    if (fd_stat.st_dev, fd_stat.st_ino) != (
        path_stat.st_dev,
        path_stat.st_ino,
    ):
        return False
    payload = _read_run_log_lock_payload(fd)
    if not _is_current_run_log_lock_payload(payload):
        return None
    unexpected_links = (
        "run log compatibility lock has unexpected hard links: "
        f"{lock_path}"
    )
    candidate_path = None
    if fd_stat.st_nlink == 2:
        candidate_path = f"{lock_path}.{payload['owner_token']}.tmp"
        try:
            candidate_stat = os.stat(candidate_path, follow_symlinks=False)
        except OSError:
            candidate_stat = None
        if candidate_stat is None or (
            fd_stat.st_dev,
            fd_stat.st_ino,
        ) != (
            candidate_stat.st_dev,
            candidate_stat.st_ino,
        ):
            raise RunLogError(unexpected_links)
    elif fd_stat.st_nlink != 1:
        raise RunLogError(unexpected_links)
    if payload["host_id"] != _run_log_host_id():
        return False
    pid = payload.get("pid")
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or _pid_is_alive(pid)
    ):
        return False
    if candidate_path is not None:
        os.unlink(candidate_path)
    return True


def _acquire_compatibility_run_log_lock(path, owner_token, deadline):
    lock_path = _run_log_lock_path(path)
    candidate_path = f"{lock_path}.{owner_token}.tmp"
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise RunLogError(
            "secure run log locking requires O_NOFOLLOW support"
        )
    try:
        candidate_fd = os.open(
            candidate_path,
            os.O_CREAT | os.O_EXCL | os.O_RDWR | nofollow,
            0o600,
        )
    except OSError as err:
        raise RunLogError(
            f"failed to create run log compatibility lock: {err}"
        ) from err
    installed = False
    try:
        _write_run_log_lock_payload(candidate_fd, owner_token)
        while True:
            try:
                os.link(candidate_path, lock_path, follow_symlinks=False)
            except FileExistsError:
                try:
                    existing_fd = os.open(
                        lock_path,
                        os.O_RDONLY | os.O_NONBLOCK | nofollow,
                    )
                except FileNotFoundError:
                    continue
                except OSError as err:
                    raise RunLogError(
                        f"failed to inspect run log compatibility lock: {err}"
                    ) from err
                try:
                    reclaimable = _compatibility_lock_is_reclaimable(
                        lock_path, existing_fd
                    )
                    if reclaimable is None and _validate_run_log_lock_file(
                        lock_path, existing_fd
                    ):
                        reclaimable = (
                            _legacy_compatibility_lock_is_reclaimable(
                                _read_run_log_lock_payload(existing_fd),
                                existing_fd,
                            )
                        )
                    if reclaimable and _unlink_run_log_lock_if_matches(
                        lock_path, existing_fd
                    ):
                        continue
                finally:
                    os.close(existing_fd)
                if time.monotonic() >= deadline:
                    raise RunLogError(
                        f"run log compatibility lock remains at {lock_path}; "
                        "verify that no other Sneeze process is writing, "
                        "then remove that lock file"
                    ) from None
                time.sleep(_LOCK_POLL_INTERVAL_S)
                continue
            except OSError as err:
                raise RunLogError(
                    f"failed to acquire run log compatibility lock: {err}"
                ) from err
            installed = True
            os.unlink(candidate_path)
            return lock_path, candidate_fd
    except Exception:
        if installed:
            try:
                _unlink_run_log_lock_if_matches(lock_path, candidate_fd)
            except OSError:
                pass
        os.close(candidate_fd)
        try:
            os.unlink(candidate_path)
        except FileNotFoundError:
            pass
        raise


def _acquire_run_log_lock(path):
    flock_path = _run_log_flock_path(path)
    directory = os.path.dirname(flock_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    deadline = time.monotonic() + _LOCK_TIMEOUT_S
    while True:
        fd = _open_run_log_lock(flock_path)
        if fd is None:
            if time.monotonic() >= deadline:
                raise RunLogError(
                    f"timed out acquiring run log lock: {flock_path}"
                ) from None
            time.sleep(_LOCK_POLL_INTERVAL_S)
            continue
        try:
            if not _validate_run_log_lock_file(flock_path, fd):
                os.close(fd)
                fd = None
                if time.monotonic() >= deadline:
                    raise RunLogError(
                        f"timed out acquiring run log lock: {flock_path}"
                    ) from None
                time.sleep(_LOCK_POLL_INTERVAL_S)
                continue
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as err:
                if err.errno not in (errno.EACCES, errno.EAGAIN):
                    raise RunLogError(
                        f"failed to acquire run log lock: {flock_path}: {err}"
                    ) from err
                os.close(fd)
                fd = None
                if time.monotonic() >= deadline:
                    raise RunLogError(
                        f"timed out acquiring run log lock: {flock_path}"
                    ) from None
                time.sleep(_LOCK_POLL_INTERVAL_S)
                continue
            if not _validate_run_log_lock_file(flock_path, fd):
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                fd = None
                if time.monotonic() >= deadline:
                    raise RunLogError(
                        f"timed out acquiring run log lock: {flock_path}"
                    ) from None
                time.sleep(_LOCK_POLL_INTERVAL_S)
                continue
            break
        except Exception:
            if fd is not None:
                os.close(fd)
            raise
    owner_token = uuid.uuid4().hex
    try:
        _write_run_log_lock_payload(fd, owner_token)
        compatibility_path, compatibility_fd = (
            _acquire_compatibility_run_log_lock(
                path,
                owner_token,
                deadline,
            )
        )
    except Exception:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        raise
    return _RunLogLock(
        flock_path,
        fd,
        owner_token,
        compatibility_path,
        compatibility_fd,
    )


def _release_run_log_lock(lock):
    if not lock:
        return
    try:
        try:
            _unlink_run_log_lock_if_matches(
                lock.compatibility_path, lock.compatibility_fd
            )
        finally:
            os.close(lock.compatibility_fd)
    finally:
        try:
            fcntl.flock(lock.fd, fcntl.LOCK_UN)
        finally:
            os.close(lock.fd)


def _write_json_array_atomic(path, entry_jsons):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=directory or None,
        delete=False,
    )
    tmp_path = handle.name
    try:
        with handle:
            handle.write("[\n")
            for index, entry_json in enumerate(entry_jsons):
                if index:
                    handle.write(",\n")
                handle.write(entry_json.strip())
            handle.write("\n]\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _backup_corrupted_run_log(path):
    index = 0
    while True:
        suffix = ".corrupt" if index == 0 else f".corrupt.{index}"
        backup_path = f"{path}{suffix}"
        try:
            fd = os.open(
                backup_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except FileExistsError:
            index += 1
            continue
        break
    try:
        with os.fdopen(fd, "wb") as target:
            with open(path, "rb") as source:
                shutil.copyfileobj(source, target)
    except Exception:
        try:
            os.unlink(backup_path)
        except OSError:
            pass
        raise
    return backup_path


def _append_json_list(path, entry_json):
    entry = entry_json.strip()
    if not entry:
        return
    lock = _acquire_run_log_lock(path)
    try:
        existing_entries = []
        if os.path.exists(path) and os.path.getsize(path) > 0:
            try:
                instances = load_run_instances([path], strict=True)
            except RunLogCorruptionError:
                _backup_corrupted_run_log(path)
                instances = load_run_instances([path], strict=False)
            existing_entries = [inst.dump_json() for inst in instances]
        existing_entries.append(entry)
        _write_json_array_atomic(path, existing_entries)
    finally:
        _release_run_log_lock(lock)


def repair_run_log(path):
    if not os.path.exists(path):
        return 0
    lock = _acquire_run_log_lock(path)
    try:
        instances = load_run_instances([path], strict=False)
        _write_json_array_atomic(
            path, [inst.dump_json() for inst in instances]
        )
    finally:
        _release_run_log_lock(lock)
    return len(instances)


def _load_json_fragments(text):
    decoder = json.JSONDecoder()
    pos = 0
    size = len(text)
    values = []
    while pos < size:
        while pos < size and text[pos] in " \t\r\n,":
            pos += 1
        if pos >= size:
            break
        if text[pos] == "]":
            pos += 1
            continue
        try:
            value, pos = decoder.raw_decode(text, pos)
        except json.JSONDecodeError:
            break
        values.append(value)
    return values


def _iter_run_log_items(text, path=None, strict=True):
    try:
        values = [json.loads(text)]
    except json.JSONDecodeError as err:
        if strict:
            recovered = _load_json_fragments(text)
            message = _format_json_decode_error(err)
            if recovered:
                count = _count_recovered_items(recovered)
                message += f"; found {count} recoverable trailing items"
            raise RunLogCorruptionError(path, message) from err
        values = _load_json_fragments(text)
    for value in values:
        if isinstance(value, list):
            yield from value
        elif isinstance(value, dict):
            if strict:
                raise RunLogCorruptionError(
                    path,
                    "top-level JSON value must be a list",
                )
            yield value
        elif strict:
            typename = type(value).__name__
            raise RunLogCorruptionError(
                path,
                f"top-level JSON value must be a list, got {typename}",
            )


_DEFAULT_REPO_ROOT = object()


def _default_repo_root():
    return find_repo_root()


@lru_cache(maxsize=8)
def _git_rev_for_root(repo_root):
    import subprocess

    if not repo_root:
        return None
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return output.decode("utf-8").strip() or None


def get_run_log_path(hostname=None, run_dir=None):
    host = hostname or HOSTNAME
    base = run_dir or SNEEZE_RUN_DIR
    return join_path(base, f"{RUN_LOG_PREFIX}{host}{RUN_LOG_SUFFIX}")


def list_run_log_paths(hostnames=None, all_hosts=False, run_dir=None):
    base = run_dir or SNEEZE_RUN_DIR
    if all_hosts:
        if not os.path.isdir(base):
            return []
        entries = []
        for name in os.listdir(base):
            if name.startswith(RUN_LOG_PREFIX) and name.endswith(
                RUN_LOG_SUFFIX
            ):
                entries.append(join_path(base, name))
        return sorted(entries)
    hosts = hostnames or [HOSTNAME]
    return [get_run_log_path(hostname=host, run_dir=base) for host in hosts]


def load_run_instances(paths, strict=True):
    instances = []
    for path in paths:
        try:
            with open(path, encoding="utf-8") as handle:
                text = handle.read()
        except (FileNotFoundError, OSError):
            continue
        for index, item in enumerate(
            _iter_run_log_items(text, path=path, strict=strict),
            start=1,
        ):
            try:
                inst = SneezeCommandRunInstance.model_validate(item)
            except Exception as err:
                if strict:
                    raise RunLogCorruptionError(
                        path,
                        f"invalid run log entry {index}: {err}",
                    ) from err
                continue
            instances.append(inst)
    return instances


def append_run_instance(instance, hostname=None, run_dir=None):
    path = get_run_log_path(hostname=hostname, run_dir=run_dir)
    _append_json_list(path, instance.dump_json())


class CommandRunContext:
    def __init__(self, argv, command=None, repo_root=_DEFAULT_REPO_ROOT):
        self.argv = list(argv) if argv else []
        self.command = command
        if repo_root is _DEFAULT_REPO_ROOT:
            repo_root = _default_repo_root()
        self.repo_root = repo_root
        self.started_at = dt.datetime.now(dt.UTC)
        self._start_perf = time.perf_counter()
        self._start_usage = _get_rusage()

    def finish(self, exit_code, error=None):
        ended_at = dt.datetime.now(dt.UTC)
        duration = time.perf_counter() - self._start_perf
        resources = _resource_delta(self._start_usage, _get_rusage())
        instance = SneezeCommandRunInstance(
            argv=self.argv,
            command=self.command,
            hostname=HOSTNAME,
            host_fqdn=HOSTFQDN,
            username=getpass.getuser(),
            pid=os.getpid(),
            cwd=os.getcwd(),
            repo_root=self.repo_root,
            git_rev=_git_rev_for_root(self.repo_root),
            started_at=self.started_at,
            ended_at=ended_at,
            duration_s=duration,
            exit_code=int(exit_code or 0),
            resources=resources,
            error_type=type(error).__name__ if error else None,
            error_message=str(error) if error else None,
        )
        append_run_instance(instance)
        return instance
