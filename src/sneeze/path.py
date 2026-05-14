import os

join = os.path.join
abspath = os.path.abspath
dirname = os.path.dirname
normpath = os.path.normpath


def join_path(*parts):
    return abspath(normpath(join(*parts)))
