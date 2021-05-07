import stat

import os
import os.path
import posixpath
import logging

class FileInfo():

    @staticmethod
    def from_json(json):
        return FileInfo(json["is_dir"],
                        json["file_size"],
                        json["modification_time"],
                        json["path"])

    @staticmethod
    def from_local(filename, *more):
        path = os.path.abspath(os.path.join(filename, *more))
        st = os.stat(path)
        return FileInfo(stat.S_ISDIR(st.st_mode),
                        st.st_size,
                        st.st_mtime,
                        path)

    def __init__(self, is_dir, size, mtime, abspath):
        self._is_dir = is_dir
        self._size = size
        self._mtime = mtime
        self._abspath = abspath

    def __repr__(self):
        return f"fi(abspath={self._abspath}, is_dir={self._is_dir}, size={self._size}, mtime={self._mtime})"

    def to_data(self):
        return { "abspath": self._abspath,
                 "size":    self._size,
                 "mtime":   self._mtime,
                 "is_dir":  self._is_dir }

    def basename(self):
        return posixpath.basename(self._abspath)

    def abspath(self):
        return self._abspath

    def relpath(self, base, requested=None):
        if requested is not None and requested[0] == "/":
            # the original request was absolute
            return self._abspath
        base = posixpath.normpath(base)
        path = self.abspath()
        if path == base:
            return "."
        if path.startswith(base):
            if base == "/":
                return path[1:]
            elif path[len(base)] == "/":
                return path[(len(base)+1):]
        return path

    def is_dir(self):
        return self._is_dir

    def mtime(self):
        return self._mtime

    def size(self):
        return self._size

    def type(self):
        return "dir" if self._is_dir else "file"

    def _check_predicate__newer_than(self, limit, _):
        return self.mtime() >= limit*1000

    def _check_predicate__older_than(self, limit, _):
        return self.mtime() <= limit*1000

    def _check_predicate__max_size(self, limit, _):
        if self.is_dir():
            return True
        return self.size() <= limit

    def _check_predicate__min_size(self, limit, _):
        if self.is_dir():
            return True
        return self.size() >= limit

    # FIXME: all those methods doing exactly the same are ugly
    def _check_predicate__name(self, pattern, _):
        return pattern.fullmatch(self.basename(), _)

    def _check_predicate__iname(self, pattern):
        return pattern.fullmatch(self.basename(), _)

    def _check_predicate__re(self, pattern):
        return pattern.search(self.basename(), _)

    def _check_predicate__ire(self, pattern, _):
        return pattern.search(self.basename())

    def _check_predicate__iwholere(self, pattern, relpath):
        return pattern.search(self.basename if relpath is None else relpath)

    def _check_predicate__wholere(self, pattern, relpath):
        return pattern.search(self.basename if relpath is None else relpath)

    def check_predicates(self, relpath=None, **predicates):
        #print(f"predicates: {predicates}")
        for key, value in predicates.items():
            if value is not None:
                if key.startswith("exclude_"):
                    method = getattr(self, f"_check_predicate__{key[len('exclude_'):]}")
                    if method(value, relpath):
                        logging.debug(f"entry {self.abspath()} has been discarded by predicate {key} with value {value}")
                        return False
                else:
                    method = getattr(self, f"_check_predicate__{key}")
                    if not method(value, relpath):
                        logging.debug(f"entry {self.abspath()} has been discarded by predicate {key} with value {value}")
                        return False
        return True
