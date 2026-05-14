from .command import Command
from .invariant import InvariantAwareObject


class InvariantAwareCommand(InvariantAwareObject, Command):
    def __init__(self, *args, **kwds):
        long_opts = kwds.get("long_opts", {})
        short_opts = kwds.get("short_opts", {})
        if "h" not in short_opts:
            short_opts["h"] = None
        for short, long in (("v", "verbose"), ("q", "quiet"), ("c", "conf")):
            attr = f"_{long}_"
            if getattr(self, attr):
                long_opts.setdefault(long, None)
                short_opts.setdefault(short, None)
        kwds["long_opts"] = long_opts
        kwds["short_opts"] = short_opts
        InvariantAwareObject.__init__(self, *args, **kwds)
        kwds.pop("long_opts", None)
        kwds.pop("short_opts", None)
        Command.__init__(self, *args, **kwds)
