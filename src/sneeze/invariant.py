import datetime as dt
import os
import re
from os.path import abspath, dirname, exists, isdir, isfile

SUFFIXES = ("Option", "Error", "Arg")
STRING_TYPES = (str,)
CLASS_TOKEN_REGEX = re.compile("[A-Z][^A-Z]*")


class Invariant(Exception):
    _arg = None
    _type = None
    _help = None
    _action = None
    _default = None
    _metavar = None
    _mandatory = True
    _opt_long = None
    _opt_short = None
    _opt_type = None
    _type_desc = None

    expected = None
    message = None

    def __init__(self, obj, name):
        self._obj = obj
        self._name = name
        self.actual = None
        self.dst_value = None
        self._configure_option_names()
        super().__init__("")

    def _configure_option_names(self):
        short = None
        long = None
        if self._arg:
            arg = self._arg
            if "/" in arg:
                short, long = arg.split("/", 1)
                short = short[1:] if short.startswith("-") else short
                long = long[2:] if long.startswith("--") else long
            elif arg.startswith("--"):
                long = arg[2:]
            elif arg.startswith("-"):
                short = arg[1:]
            else:
                raise ValueError(f"invalid option arg: {arg}")
        else:
            long = self._name.replace("_", "-")
            for char in self._name:
                if char == "_":
                    continue
                if char not in self._obj._short_opts:
                    short = char
                    break

        if long:
            if long in self._obj._long_opts:
                raise ValueError(f"duplicate long option: {long}")
            self._obj._long_opts[long] = self
        if short:
            if short in self._obj._short_opts:
                short = None
            else:
                self._obj._short_opts[short] = self
        self._opt_long = long
        self._opt_short = short

        if not self._opt_type and self._type:
            if self._type in STRING_TYPES:
                self._opt_type = "string"
            elif self._type is int:
                self._opt_type = "int"
            elif self._type is float:
                self._opt_type = "float"

        if self._metavar is None and self._opt_type:
            self._metavar = self._name.upper()

    def _try_save(self, value, retval=True):
        attr = "_" + self._name
        if hasattr(self._obj, attr):
            setattr(self._obj, attr, value)
        return retval

    def _test(self):
        return self.actual == self.expected

    def _validate(self, new_value):
        self.actual = new_value
        if self._test():
            return
        message = self.message
        if not message:
            message = (
                f"{self._name} is invalid: expected: "
                f"{self.expected!r}, got: {self.actual!r}"
            )
        super().__init__(message)
        raise self


class BoolInvariant(Invariant):
    _type = bool
    _metavar = None
    _action = "store_true"
    _default = False

    def _test(self):
        return True


class StringInvariant(Invariant):
    _type = str
    _type_desc = "string"
    _minlen = 1
    _maxlen = 1024

    @property
    def expected(self):
        return (
            f"{self._type_desc} with length between "
            f"{self._minlen} and {self._maxlen} characters"
        )

    def _test(self):
        if not isinstance(self.actual, self._type):
            return False
        return self._minlen <= len(self.actual) <= self._maxlen


class CSVStringInvariant(Invariant):
    _type = str
    expected = "one or more strings separated by ','"

    def _test(self):
        try:
            values = [str(item) for item in self.actual.split(",")]
        except (ValueError, AttributeError):
            return False
        if not values:
            return False
        return self._try_save(values)


class PositiveIntegerInvariant(Invariant):
    _type = int
    expected = "an integer greater than 0"

    def _test(self):
        try:
            value = int(self.actual)
        except (TypeError, ValueError):
            return False
        if value <= 0:
            return False
        return self._try_save(value)


class NonNegativeIntegerInvariant(Invariant):
    _type = int
    expected = "an integer greater than or equal to 0"

    def _test(self):
        try:
            value = int(self.actual)
        except (TypeError, ValueError):
            return False
        if value < 0:
            return False
        return self._try_save(value)


class PathInvariant(StringInvariant):
    expected = "a valid existing file path"
    _minlen = 1

    def _test(self):
        if not super()._test():
            return False
        path = abspath(self.actual)
        if not isfile(path):
            return False
        return self._try_save(path)


class OutPathInvariant(StringInvariant):
    expected = "a valid output path"
    _minlen = 1
    _mkdir = True

    def _test(self):
        if not super()._test():
            return False
        path = abspath(self.actual)
        base = dirname(path)
        if base and not exists(base) and self._mkdir:
            os.makedirs(base)
        return self._try_save(path)


class DirectoryInvariant(StringInvariant):
    expected = "a valid existing directory"
    _minlen = 1

    def _test(self):
        if not super()._test():
            return False
        path = abspath(self.actual)
        if not isdir(path):
            return False
        return self._try_save(path)


class MkDirectoryInvariant(StringInvariant):
    expected = "a valid directory"
    _minlen = 1

    def _test(self):
        if not super()._test():
            return False
        path = abspath(self.actual)
        os.makedirs(path, exist_ok=True)
        return self._try_save(path)


class DateInvariant(Invariant):
    _type_desc = "datetime"
    _date_format = "%Y-%m-%d"
    expected = "a date in the format 'YYYY-MM-DD'"

    def _test(self):
        try:
            value = dt.datetime.strptime(
                self.actual,
                self._date_format,
            ).date()
        except (TypeError, ValueError):
            return False
        return self._try_save(value)


class EndDateInvariant(DateInvariant):
    def _test(self):
        if not super()._test():
            return False
        start_date = getattr(self._obj, "_start_date", None)
        end_date = getattr(self._obj, "_end_date", None)
        if start_date and end_date and start_date > end_date:
            self.message = (
                f"end date ({self.actual}) is earlier than "
                f"start date ({start_date.strftime(self._date_format)})"
            )
            return False
        return True


def _invariant_name_from_class(class_name):
    tokens = CLASS_TOKEN_REGEX.findall(class_name)
    if tokens and tokens[-1] in SUFFIXES:
        tokens = tokens[:-1]
    return "_".join(token.lower() for token in tokens)


class InvariantAwareObject:
    def __init__(self, *args, **kwds):
        self._long_opts = kwds.get("long_opts", {})
        self._short_opts = kwds.get("short_opts", {})
        classes = []
        seen = set()
        for cls in reversed(self.__class__.mro()):
            for attr_name, value in cls.__dict__.items():
                if attr_name in seen:
                    continue
                if not isinstance(value, type):
                    continue
                if not attr_name.endswith(SUFFIXES):
                    continue
                if not issubclass(value, Invariant):
                    continue
                seen.add(attr_name)
                classes.append((_invariant_name_from_class(attr_name), value))

        self._invariant_order = list(enumerate(name for name, _ in classes))
        self._invariant_classes = {name: cls for name, cls in classes}
        self._invariants = {
            name: cls(self, name)
            for name, cls in self._invariant_classes.items()
        }
        self._invariants_processed = []

    def __setattr__(self, name, new_value):
        object.__setattr__(self, name, new_value)
        if hasattr(self, "_invariants") and name in self._invariants:
            invariant = self._invariants[name]
            invariant._validate(new_value)
            self._invariants_processed.append(invariant)
