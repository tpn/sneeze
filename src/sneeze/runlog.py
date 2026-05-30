import datetime as dt
import getpass
import json
import os
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass
from functools import lru_cache

from .config import HOSTFQDN, HOSTNAME, SNEEZE_RUN_DIR
from .path import find_repo_root, join_path

RUN_LOG_PREFIX = "sne-"
RUN_LOG_SUFFIX = ".json"
RUN_LOG_LOCK_SUFFIX = ".lock"
_LOCK_POLL_INTERVAL_S = 0.05
_LOCK_STALE_GRACE_S = 5.0
_LOCK_TIMEOUT_S = float(os.environ.get("SNEEZE_RUN_LOG_LOCK_TIMEOUT", "30"))


class RunLogError(Exception):
    pass


class RunLogCorruptionError(RunLogError):
    def __init__(self, path, message):
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}")


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


def _current_lock_payload():
    return json.dumps(
        {
            "pid": os.getpid(),
            "hostname": HOSTNAME,
            "acquired_at": dt.datetime.now(dt.UTC).isoformat(),
        }
    )


def _pid_is_alive(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except (PermissionError, OSError):
        return True
    return True


def _read_lock_payload(path):
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, OSError, json.JSONDecodeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _break_stale_run_log_lock(lock_path):
    payload = _read_lock_payload(lock_path)
    if isinstance(payload, dict):
        pid = payload.get("pid")
        if isinstance(pid, int) and _pid_is_alive(pid):
            return False
    else:
        try:
            age = time.time() - os.path.getmtime(lock_path)
        except OSError:
            return False
        if age < _LOCK_STALE_GRACE_S:
            return False
    try:
        os.unlink(lock_path)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return True


def _acquire_run_log_lock(path):
    lock_path = _run_log_lock_path(path)
    directory = os.path.dirname(lock_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    deadline = time.monotonic() + _LOCK_TIMEOUT_S
    while True:
        try:
            fd = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise RunLogError(
                    f"timed out acquiring run log lock: {lock_path}"
                ) from None
            if _break_stale_run_log_lock(lock_path):
                continue
            time.sleep(_LOCK_POLL_INTERVAL_S)
            continue
        try:
            os.write(fd, _current_lock_payload().encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        return lock_path


def _release_run_log_lock(lock_path):
    if not lock_path:
        return
    try:
        os.unlink(lock_path)
    except FileNotFoundError:
        return


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
    lock_path = _acquire_run_log_lock(path)
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
        _release_run_log_lock(lock_path)


def repair_run_log(path):
    if not os.path.exists(path):
        return 0
    lock_path = _acquire_run_log_lock(path)
    try:
        instances = load_run_instances([path], strict=False)
        _write_json_array_atomic(
            path, [inst.dump_json() for inst in instances]
        )
    finally:
        _release_run_log_lock(lock_path)
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
