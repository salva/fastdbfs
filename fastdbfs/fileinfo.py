import stat

import os
import os.path

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
        return os.path.basename(self._abspath)

    def abspath(self):
        return self._abspath

    def relpath(self, base):
        base = os.path.normpath(base)
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

