import importlib
import inspect
import optparse
import os
import sys
import textwrap
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from textwrap import dedent

import sneeze
import sneeze.config

from .command import (
    DEFAULT_LOG_LEVEL_NAME,
    STANDARD_LOG_LEVEL_NAMES,
    Command,
    CommandError,
)
from .config import ConfigObjectAlreadyCreated, get_config
from .invariant import Invariant
from .path import find_repo_root
from .plugin import discover_plugins, import_plugin_modules
from .runlog import CommandRunContext, RunLogError
from .util import (
    DecayDict,
    Dict,
    Options,
    add_linesep_if_missing,
    prepend_error_if_missing,
)

INTERACTIVE = False
DEFAULT_PROGRAM_NAME = "sne"
DEFAULT_MODULE_NAMES = "sneeze"


@dataclass
class ModuleRecord:
    namespace: str
    commands: object
    config: object
    plugin_name: str | None = None
    is_core: bool = False


class CommandLine:
    def __init__(
        self,
        program_name,
        command_class,
        config_class,
        display_name=None,
        display_shortname=None,
    ):
        self.mandatory_opts = OrderedDict()
        self.config_class = config_class
        self.program_name = program_name
        self.command_class = command_class
        self.command_classname = command_class.__name__
        self.command = self.command_class(sys.stdin, sys.stdout, sys.stderr)
        self.name = display_name or self.command.name
        self.shortname = display_shortname
        if display_shortname is None:
            self.shortname = self.command.shortname
        self.prog = f"{self.program_name} {self.name}"
        self.parser = None
        self.conf = None
        self.args = []
        self.options = None

    def add_option(self, *args, **kwds):
        if kwds.get("mandatory"):
            self.mandatory_opts[args] = kwds["dest"]
            del kwds["mandatory"]
        self.parser.add_option(*args, **kwds)

    def usage_error(self, msg):
        self.parser.print_help()
        sys.stderr.write(f"\nerror: {msg}\n")
        self.parser.exit(status=1)

    def _add_parser_options(self):
        cmd = self.command
        if not hasattr(cmd, "_invariants"):
            return
        for _, name in cmd._invariant_order:
            invariant = cmd._invariants[name]
            args = []
            if invariant._opt_short:
                args.append("-" + invariant._opt_short)
            if invariant._opt_long:
                args.append("--" + invariant._opt_long)
            fields = ("help", "action", "default", "metavar", "mandatory")
            kwds = Dict()
            kwds.dest = name
            for field in fields:
                value = getattr(invariant, "_" + field, None)
                if value is None:
                    continue
                if field == "mandatory" and not value:
                    continue
                kwds[field] = value if not callable(value) else value()
            self.add_option(*args, **kwds)

    def run(self, args):
        kwds = Dict()
        kwds.prog = self.prog
        usage = getattr(self.command, "_usage_", None)
        description = getattr(self.command, "_description_", None)
        if usage:
            kwds.usage = usage
        if description:
            kwds.description = description
        elif self.command.__doc__:
            kwds.description = textwrap.dedent(self.command.__doc__)
        self.parser = optparse.OptionParser(**kwds)
        if getattr(self.command, "_disable_interspersed_args_", False):
            self.parser.disable_interspersed_args()
        if self.command._verbose_:
            self.parser.add_option(
                "-v",
                "--verbose",
                dest="verbose",
                action="store_true",
                default=False,
                help="run in verbose mode [default: %default]",
            )
        if self.command._quiet_:
            self.parser.add_option(
                "-q",
                "--quiet",
                dest="quiet",
                action="store_true",
                default=False,
                help="run in quiet mode [default: %default]",
            )
        if self.command._conf_:
            self.parser.add_option(
                "-c",
                "--conf",
                metavar="FILE",
                help="use alternate configuration file FILE",
            )
        if self.command._log_level_:
            choices = ", ".join(STANDARD_LOG_LEVEL_NAMES)
            self.parser.add_option(
                "--log-level",
                dest="log_level",
                metavar="LEVEL",
                default=None,
                help=(
                    f"set logging level ({choices}) "
                    f"[default: {DEFAULT_LOG_LEVEL_NAME}]"
                ),
            )
        self._add_parser_options()
        opts, self.args = self.parser.parse_args(args)
        argc = getattr(self.command, "_argc_", 0)
        vargc = getattr(self.command, "_vargc_", None)
        if vargc is not True and argc and len(self.args) != argc:
            self.usage_error("invalid number of arguments")
        self.options = Options(opts.__dict__)
        for opt, name in self.mandatory_opts.items():
            if opts.__dict__.get(name) is None:
                self.usage_error(f"{'/'.join(opt)} is mandatory")
        conf_file = None
        if self.command._conf_:
            conf_file = self.options.conf
            if conf_file and not os.path.exists(conf_file):
                self.usage_error(
                    f"configuration file '{conf_file}' does not exist"
                )
        if self.command._log_level_:
            try:
                self.command.set_log_level(self.options.log_level)
            except ValueError as exc:
                self.usage_error(str(exc))
            self.options.log_level = self.command.log_level
        try:
            self.conf = self.config_class(options=self.options)
            self.conf.load(filename=conf_file)
        except ConfigObjectAlreadyCreated:
            self.conf = get_config()
        self.command.interactive = INTERACTIVE
        self.command.conf = self.conf
        self.command.args = self.args
        self.command.options = self.options
        self.command.start()


class CLI:
    __unknown_subcommand__ = "Unknown subcommand '%s'"
    __usage__ = "Type '%prog help' for usage."
    __help__ = """\
        Type '%prog <subcommand> help' for help on a specific subcommand.

        Available subcommands:"""

    def __init__(self, *args, **kwds):
        opts = DecayDict(**kwds)
        self.args = list(args) if args else []
        self.program_name = opts.program_name
        self.module_names = opts.module_names or []
        self.args_queue = opts.get("args_queue", None)
        self.feedback_queue = opts.get("feedback_queue", None)
        self.introspect = bool(opts.get("introspect", False))
        self.auto_plugins = bool(opts.get("auto_plugins", True))
        opts.assert_empty(self)
        self.returncode = 0
        self.commandline = None
        self.modules = []
        self._help = self.__help__
        self._commands_by_name = {}
        self._commands_by_shortname = {}
        self._import_modules()
        self._load_commands()
        if self.introspect:
            return
        if not self.args_queue:
            if self.args:
                self.run()
            else:
                self.help()

    def run(self):
        if not self.args_queue:
            self._process_commandline()
            return
        self._process_queue()

    def _process_queue(self):
        from queue import Empty

        cmdlines = {}
        while True:
            try:
                args = self.args_queue.get_nowait()
            except Empty:
                break
            raw_args = list(args)
            cmdline_norm = raw_args[0].lower() if raw_args else None
            run_ctx = CommandRunContext(
                argv=[self.program_name] + raw_args,
                command=cmdline_norm,
                repo_root=find_repo_root(),
            )
            error = None
            exit_code = 0
            try:
                cmdline = args.pop(0).lower()
                if cmdline not in cmdlines:
                    cmdlines[cmdline] = self._find_commandline(cmdline)
                cl = cmdlines[cmdline]
                if not cl:
                    error = CommandError(
                        self.__unknown_subcommand__ % cmdline
                    )
                    self._error(
                        os.linesep.join(
                            (
                                self.__unknown_subcommand__ % cmdline,
                                self.__usage__,
                            )
                        )
                    )
                    exit_code = self.returncode or 1
                    self.args_queue.task_done()
                    continue
                cl.run(args)
                self.args_queue.task_done()
            except BaseException as err:
                error = err
                exit_code = err.code if isinstance(err, SystemExit) else 1
                raise
            finally:
                if not isinstance(error, RunLogError):
                    self._finish_run_log(run_ctx, exit_code, error)

    def _import_modules(self):
        include_core = True
        module_names = list(self.module_names)
        for name in list(module_names):
            if name == "-sneeze":
                include_core = False
                module_names.remove(name)
            elif name == "sneeze":
                include_core = False
        if include_core:
            module_names.insert(0, "sneeze")
        for namespace in module_names:
            commands = importlib.import_module(f"{namespace}.commands")
            config = importlib.import_module(f"{namespace}.config")
            self.modules.append(
                ModuleRecord(
                    namespace=namespace,
                    commands=commands,
                    config=config,
                    plugin_name=None,
                    is_core=namespace == "sneeze",
                )
            )
        if self.auto_plugins:
            for spec in discover_plugins():
                if spec.package == "sneeze":
                    continue
                commands, config = import_plugin_modules(spec)
                self.modules.append(
                    ModuleRecord(
                        namespace=spec.package,
                        commands=commands,
                        config=config,
                        plugin_name=spec.username.replace("-", "_"),
                        is_core=False,
                    )
                )

    def _find_command_subclasses(self):
        results = []
        for record in self.modules:
            for name, attr in inspect.getmembers(
                record.commands, inspect.isclass
            ):
                if name.startswith("_") or attr is Command:
                    continue
                if not issubclass(attr, Command):
                    continue
                if attr.__module__ != record.commands.__name__:
                    continue
                command = attr(sys.stdin, sys.stdout, sys.stderr)
                results.append(
                    (record, command.name, command.shortname, attr)
                )
        return sorted(
            results,
            key=lambda item: (
                0 if item[0].is_core else 1,
                item[0].plugin_name or "",
                item[1],
                item[3].__name__,
            ),
        )

    def _load_commands(self):
        subclasses = self._find_command_subclasses()
        name_counts = defaultdict(int)
        for record, name, _, _ in subclasses:
            name_counts[name] += 1
            if record.is_core:
                name_counts[name] += 1000
        pending = []
        for record, name, shortname, command_class in subclasses:
            display_name = name
            display_shortname = shortname
            if not record.is_core and name_counts[name] > 1:
                display_name = f"{record.plugin_name}-{name}"
                if shortname:
                    display_shortname = f"{record.plugin_name}-{shortname}"
            pending.append(
                (
                    record,
                    display_name,
                    display_shortname,
                    command_class,
                )
            )

        for record, display_name, display_shortname, command_class in pending:
            if display_name in self._commands_by_name:
                continue
            config_class = record.config.Config
            cl = CommandLine(
                self.program_name,
                command_class,
                config_class,
                display_name=display_name,
                display_shortname=display_shortname,
            )
            self._commands_by_name[cl.name] = cl
            if (
                cl.shortname
                and cl.shortname not in self._commands_by_shortname
            ):
                self._commands_by_shortname[cl.shortname] = cl
            self._help += self._helpstr(cl.name, cl.shortname)
        self._help += self._helpstr("version", None)
        self._commands_by_name["version"] = None
        if not self.introspect:
            self._maybe_write_command_map()

    def _maybe_write_command_map(self):
        if os.environ.get("SNEEZE_WRITE_COMMAND_MAP") != "1":
            return
        repo_root = find_repo_root()
        if not repo_root:
            return
        lines = []
        for name in sorted(self._commands_by_name):
            cl = self._commands_by_name[name]
            if not cl:
                continue
            suffix = f" ({cl.shortname})" if cl.shortname else ""
            lines.append(f"{cl.name}{suffix} -> {cl.command_classname}")
        path = os.path.join(repo_root, "COMMAND-MAP.md")
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("\n".join(lines) + "\n")

    def _helpstr(self, name, shortname=None):
        value = os.linesep + (" " * 12) + name
        if shortname:
            value += f" ({shortname})"
        return value

    def _find_commandline(self, cmdline):
        return self._commands_by_name.get(
            cmdline,
            self._commands_by_shortname.get(cmdline),
        )

    def _process_commandline(self):
        args = self.args
        raw_args = list(args)
        cmdline_raw = raw_args[0] if raw_args else ""
        cmdline_norm = cmdline_raw.lower() if cmdline_raw else None
        argv0 = sys.argv[0] if sys.argv else self.program_name
        run_ctx = CommandRunContext(
            argv=[argv0] + raw_args,
            command=cmdline_norm,
            repo_root=find_repo_root(),
        )
        error = None
        exit_code = None
        cmdline = args.pop(0).lower()
        try:
            if cmdline and cmdline[0] != "_":
                if "-" not in cmdline and hasattr(self, cmdline):
                    getattr(self, cmdline)(args)
                    exit_code = 0
                    return self._exit(0)
                if cmdline in ("-v", "-V", "--version"):
                    self.version()
                    exit_code = self.returncode or 0
                    return self._exit(exit_code)
                else:
                    cl = self.commandline = self._find_commandline(cmdline)
                    if cl:
                        try:
                            cl.run(args)
                            exit_code = 0
                            return self._exit(0)
                        except (CommandError, Invariant, RunLogError) as err:
                            error = err
                            self._commandline_error(cl, str(err))
                            exit_code = self.returncode or 1
            if not self.returncode:
                if not error:
                    error = CommandError(
                        self.__unknown_subcommand__ % cmdline
                    )
                self._error(
                    os.linesep.join(
                        (
                            self.__unknown_subcommand__ % cmdline,
                            self.__usage__,
                        )
                    )
                )
                exit_code = self.returncode or 1
        except SystemExit as err:
            error = err
            exit_code = err.code if isinstance(err.code, int) else 1
            self._exit(exit_code)
        finally:
            if exit_code is None:
                exit_code = self.returncode or 1
            if not isinstance(error, RunLogError):
                self._finish_run_log(run_ctx, exit_code, error)

    def _finish_run_log(self, run_ctx, exit_code, error):
        try:
            run_ctx.finish(exit_code, error=error)
        except RunLogError as err:
            self._runlog_error(err)

    def _exit(self, code):
        self.returncode = code

    def _commandline_error(self, cl, msg):
        msg = f"{self.program_name} {cl.name} failed: {msg}"
        sys.stderr.write(prepend_error_if_missing(msg))
        return self._exit(1)

    def _error(self, msg):
        sys.stderr.write(
            add_linesep_if_missing(
                dedent(msg).replace("%prog", self.program_name)
            )
        )
        return self._exit(1)

    def _runlog_error(self, err):
        msg = f"{self.program_name} run log failed: {err}"
        sys.stderr.write(prepend_error_if_missing(msg))
        return self._exit(1)

    def usage(self, args=None):
        self._error(self.__usage__)

    def version(self, args=None):
        sys.stdout.write(add_linesep_if_missing(sneeze.__version__))
        return self._exit(0)

    def help(self, args=None):
        if args:
            help_args = [args.pop(0), "-h"]
            if args:
                help_args += args
            self.args = help_args
            self._process_commandline()
        else:
            self._error(self._help + os.linesep)


def _prefix_default_cli_args(args):
    return [DEFAULT_PROGRAM_NAME, DEFAULT_MODULE_NAMES] + list(args)


def _ensure_default_cli_args(args):
    args = list(args)
    if len(args) >= 3:
        return args
    return _prefix_default_cli_args(args)


def extract_command_args_and_kwds(*args_):
    args = [item for item in args_]
    kwds = {
        "program_name": args.pop(0),
        "module_names": (
            [item for item in args.pop(0).split(",")] if args else None
        ),
    }
    return args, kwds


def run(*args_, **kwds):
    global INTERACTIVE
    previous_interactive = INTERACTIVE
    interactive_request = False
    if len(args_) == 1 and isinstance(args_[0], str):
        args_ = tuple(args_[0].split(" "))
        interactive_request = True
    INTERACTIVE = interactive_request
    try:
        args, parsed_kwds = extract_command_args_and_kwds(*args_)
        parsed_kwds.update(kwds)
        sneeze.config._clear_config_if_already_created()
        cli = CLI(*args, **parsed_kwds)
        if interactive_request and cli.commandline:
            return cli.commandline.command
        return cli
    finally:
        INTERACTIVE = previous_interactive


def main(argv=None):
    args = _prefix_default_cli_args(sys.argv[1:] if argv is None else argv)
    cli = run(*args)
    sys.exit(cli.returncode)


if __name__ == "__main__":
    main()
