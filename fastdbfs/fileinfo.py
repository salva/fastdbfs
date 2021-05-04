import stat

import os
import os.path
import posixpath

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

    def _check_predicate__newer_than(self, limit):
        return self.mtime() >= limit*1000

    def _check_predicate__older_than(self, limit):
        return self.mtime() <= limit*1000

    def _check_predicate__max_size(self, limit):
        if self.is_dir():
            return True
        return self.size() <= limit

    def _check_predicate__min_size(self, limit):
        if self.is_dir():
            return True
        return self.size() >= limit

    def check_predicates(self, **predicates):
        #print(f"predicates: {predicates}")
        for key, value in predicates.items():
            if value is not None:
                method = getattr(self, f"_check_predicate__{key}")
                if not method(value):
                    return False
        return True
