import os

join = os.path.join
abspath = os.path.abspath
dirname = os.path.dirname
normpath = os.path.normpath


def join_path(*parts):
    return abspath(normpath(join(*parts)))


def find_repo_root(start_dir=None):
    current = abspath(start_dir or os.getcwd())
    while True:
        if os.path.exists(join(current, ".git")):
            return current
        parent = dirname(current)
        if parent == current:
            return None
        current = parent
