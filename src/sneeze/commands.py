import os
import shlex

from .command import CommandError
from .commandinvariant import InvariantAwareCommand
from .invariant import (
    BoolInvariant,
    CSVStringInvariant,
    DateInvariant,
    EndDateInvariant,
    NonNegativeIntegerInvariant,
    StringInvariant,
)
from .plugin import (
    PluginError,
    pip_install_plugin,
    pip_uninstall_plugin,
    resolve_plugin_install_target,
    scaffold_plugin,
)
from .runlog import list_run_log_paths, load_run_instances


class RunHistoryCommand(InvariantAwareCommand):
    """
    Show command run history from sne run logs.
    """

    _shortname_ = "rh"

    all_hosts = None

    class AllHostsArg(BoolInvariant):
        _help = "Scan all host logs. [default: %default]"
        _mandatory = False
        _default = False

    hostnames = None
    _hostnames = None

    class HostnamesArg(CSVStringInvariant):
        _help = "Hostnames to include (comma-separated)."
        _mandatory = False

    username = None

    class UsernameArg(StringInvariant):
        _help = "Filter by username."
        _mandatory = False

    command = None

    class CommandArg(StringInvariant):
        _help = "Filter by command name."
        _mandatory = False

    argv_contains = None

    class ArgvContainsArg(StringInvariant):
        _help = "Filter by substring in argv."
        _mandatory = False

    exit_code = None
    _exit_code = None

    class ExitCodeArg(NonNegativeIntegerInvariant):
        _help = "Filter by exit code."
        _mandatory = False

    git_rev = None

    class GitRevArg(StringInvariant):
        _help = "Filter by git rev (prefix ok)."
        _mandatory = False

    start_date = None
    _start_date = None

    class StartDateArg(DateInvariant):
        _help = "Only include runs on/after date (YYYY-MM-DD)."
        _mandatory = False

    end_date = None
    _end_date = None

    class EndDateArg(EndDateInvariant):
        _help = "Only include runs on/before date (YYYY-MM-DD)."
        _mandatory = False

    limit = None
    _limit = None

    class LimitArg(NonNegativeIntegerInvariant):
        _help = "Limit number of results. [default: %default]"
        _mandatory = False
        _default = 100

    oldest_first = None

    class OldestFirstArg(BoolInvariant):
        _help = "Show oldest first. [default: %default]"
        _mandatory = False
        _default = False

    summary = None

    class SummaryArg(BoolInvariant):
        _help = "Only show summary counts. [default: %default]"
        _mandatory = False
        _default = False

    friendly = None

    class FriendlyArg(BoolInvariant):
        _help = "Show compact per-host command history. [default: %default]"
        _mandatory = False
        _default = False

    friendly_timestamps = None

    class FriendlyTimestampsArg(BoolInvariant):
        _help = "Prefix friendly output with timestamps. [default: %default]"
        _mandatory = False
        _default = False

    def run(self):
        host_filter = None
        if self._hostnames:
            host_filter = [host.strip() for host in self._hostnames if host]
        paths = list_run_log_paths(
            hostnames=host_filter,
            all_hosts=self.all_hosts,
        )
        instances = load_run_instances(paths)
        if not instances:
            self._out("no run history found")
            return

        filtered = []
        command_filter = self.command.lower() if self.command else None
        for inst in instances:
            if host_filter and inst.hostname not in host_filter:
                continue
            if self.username and inst.username != self.username:
                continue
            if command_filter and inst.command != command_filter:
                continue
            if self.argv_contains and self.argv_contains not in " ".join(
                inst.argv
            ):
                continue
            if (
                self._exit_code is not None
                and inst.exit_code != self._exit_code
            ):
                continue
            if self.git_rev:
                if not inst.git_rev or not inst.git_rev.startswith(
                    self.git_rev
                ):
                    continue
            if self._start_date and inst.started_at.date() < self._start_date:
                continue
            if self._end_date and inst.started_at.date() > self._end_date:
                continue
            filtered.append(inst)

        filtered.sort(key=lambda item: item.started_at)
        if not self.oldest_first:
            filtered.reverse()
        if self._limit and self._limit > 0:
            filtered = filtered[: self._limit]

        if self.summary:
            counts = {}
            for inst in filtered:
                key = inst.command or "<unknown>"
                counts[key] = counts.get(key, 0) + 1
            self._out(f"runs: {len(filtered)}")
            for key in sorted(counts):
                self._out(f"{key}: {counts[key]}")
            return

        if self.friendly or self.friendly_timestamps:
            self._write_friendly(filtered)
            return

        for inst in filtered:
            self._out(inst.dump_json())

    def _write_friendly(self, instances):
        by_host = {}
        for inst in instances:
            by_host.setdefault(inst.hostname, []).append(inst)
        for host in sorted(by_host):
            self._out(f"{host}:")
            for inst in by_host[host]:
                argv = list(inst.argv or [])
                if argv:
                    argv[0] = "sne"
                cmdline = " ".join(shlex.quote(arg) for arg in argv)
                if self.friendly_timestamps:
                    timestamp = inst.started_at
                    try:
                        timestamp = timestamp.astimezone()
                    except Exception:
                        pass
                    stamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    self._out(f"    [{stamp}] {cmdline}")
                else:
                    self._out(f"    {cmdline}")


class InitPlugin(InvariantAwareCommand):
    """
    Bootstrap a Sneeze plugin project.
    """

    _argc_ = 1

    output_dir = None
    _output_dir = None

    class OutputDirArg(StringInvariant):
        _arg = "--output-dir"
        _help = "Directory to create. Defaults to ~/src/sneeze-plugin-USER."
        _mandatory = False

    force = None

    class ForceArg(BoolInvariant):
        _help = (
            "Replace scaffold files that already exist. [default: %default]"
        )
        _mandatory = False
        _default = False

    no_git = None

    class NoGitArg(BoolInvariant):
        _help = (
            "Do not run git init in the plugin directory. [default: %default]"
        )
        _mandatory = False
        _default = False

    def run(self):
        username = self.args[0]
        try:
            path = scaffold_plugin(
                username,
                output_dir=self._output_dir,
                force=self.force,
                init_git=not self.no_git,
            )
        except PluginError as exc:
            raise CommandError(str(exc)) from exc
        self._out(f"initialized plugin at {path}")


class InstallPlugin(InvariantAwareCommand):
    """
    Install a Sneeze plugin.
    """

    _argc_ = 1
    _shortname_ = "ipl"

    src_dir = None
    _src_dir = None

    class SrcDirArg(StringInvariant):
        _arg = "--src-dir"
        _help = "Source directory containing local sibling plugin repos."
        _mandatory = False

    no_editable = None

    class NoEditableArg(BoolInvariant):
        _help = "Do not install local directories in editable mode."
        _mandatory = False
        _default = False

    def run(self):
        spec = self.args[0]
        try:
            target = resolve_plugin_install_target(
                spec, src_dir=self._src_dir
            )
        except PluginError as exc:
            raise CommandError(str(exc)) from exc
        result = pip_install_plugin(target, editable=not self.no_editable)
        if result.returncode:
            raise CommandError(
                f"pip install failed with exit {result.returncode}"
            )
        mode = (
            "editable "
            if os.path.isdir(target) and not self.no_editable
            else ""
        )
        self._out(f"installed {mode}plugin from {target}")


class RemovePlugin(InvariantAwareCommand):
    """
    Remove an installed Sneeze plugin.
    """

    _argc_ = 1

    def run(self):
        name = self.args[0]
        result = pip_uninstall_plugin(name)
        if result.returncode:
            raise CommandError(
                f"pip uninstall failed with exit {result.returncode}"
            )
        self._out(f"removed plugin {name}")
