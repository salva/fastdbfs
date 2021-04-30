import sys
import cmd
import configparser
import os
import os.path
import traceback
import time
import shlex
import pathlib
import subprocess
import tempfile

from fastdbfs.dbfs import DBFS, Disconnected

class CLI(cmd.Cmd):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_cfg()
        self._dbfs = Disconnected()
        self._set_prompt()
        self._debug = True

    def cfg(self, section, key, default=None):
        try:
            return self._cfg[section][key]
        except:
            if default is not None:
                return default
            raise Exception(f"Configuration entry {key} missing in section {section}")

    def cfg_int(self, section, key):
        v = self.cfg(section, key)
        try:
            return int(v)
        except:
            raise Exception(f"Configuration entry {key} is not an integer")

    def _init_cfg(self):
        self._cfg = configparser.ConfigParser()
        self._cfg["fastdbfs"] = {
            "workers": 8,
            "chunk_size": 1048576,
            "max_retries": 10,
            "pager": "less",
            "error_delay": 10,
            "error_delay_increment": 10
        }

        home = os.path.expanduser("~")
        fns = [os.path.join(home, fn) for fn in (".databrickscfg", ".fastdbfs", ".config/fastdbfs")]
        self._cfg.read(fns)

    def _tell_error(self, msg):
        _, ex, trace = sys.exc_info()
        print(f"{msg}: {ex}")
        if self._debug:
            print("Stack trace:")
            traceback.print_tb(trace)

    def do_open(self, arg):
        try:
            id, = self._parse(arg, "DEFAULT")
            dbfs = DBFS(id,
                        host = self.cfg(id, "host"),
                        cluster_id = self.cfg(id, "cluster_id"),
                        token = self.cfg(id, "token"),
                        chunk_size = self.cfg_int("fastdbfs", "chunk_size"),
                        workers = self.cfg_int("fastdbfs", "workers"),
                        max_retries = self.cfg_int("fastdbfs", "max_retries"))

            dbfs.check()
            self._dbfs = dbfs
        except:
            self._tell_error(f"Unable to open {arg}")

    def do_cd(self, arg):
        try:
            path, = self._parse(arg, "/")
            self._dbfs.cd(path)
        except:
            self._tell_error(f"{path}: unable to change dir")

    def do_mkcd(self, arg):
        self.do_mkdir(arg)
        self.do_cd(arg)

    def _format_size(self, size):
        if (size >= 1073741824):
            return "%.1fG" % (size / 1073741824)
        if (size > 1024 * 1024):
            return "%.1fM" % (size / 1048576)
        if (size > 1024):
            return "%.1fK" % (size / 1024)
        return str(int(size))

    def _format_time(self, mtime):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime/1000))

    def do_mkdir(self, arg):
        try:
            path, = self._parse(arg, min=1)
            self._dbfs.mkdir(path)
        except:
            self._tell_error(f"{path}: mkdir failed")

    def do_ls(self, arg):
        try:
            path, = self._parse(arg, ".")
            # cols are type, size, mtime and path
            type_len = 3
            size_len = 1
            mtime_len = 1
            table = []
            for e in self._dbfs.ls(path):
                row = (e.type(),
                       self._format_size(e.size()),
                       self._format_time(e.mtime()),
                       e.basename())

                # print(f"row: {row}")
                type_len = max(type_len, len(row[0]))
                size_len = max(size_len, len(row[1]))
                mtime_len = max(mtime_len, len(row[2]))
                table.append(row)

            fmt = "{:>"+str(type_len)+"} {:>"+str(size_len)+"} {:>"+str(mtime_len)+"} {}"
            for e in table:
                print(fmt.format(*e))

        except:
            self._tell_error(f"{arg}: unable to list directory")

    def do_lcd(self, arg):
        try:
            path, = self._parse(arg, os.path.expanduser("~"))
            os.chdir(path)
        except:
            self._tell_error(f"{arg}: unable to change dir")

    def do_lpwd(self, arg):
        self._parse(arg)
        try:
            print(os.getcwd())
        except:
            self._tell_error("getcwd failed")

    def _local_mkdir(self, path):
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    def do_put(self, arg):
        try:
            src, target = self._parse(arg, ".", min=1)
            try:
                # We check whether the target exists and if it is a
                # directory.  If it is a directory we compose the name
                # using the src basename and check the target again.
                for first in (True, False):
                    fi = self._dbfs.get_status(target)
                    if first and fi.is_dir():
                        target = os.path.join(target, os.path.basename(src))
                    else:
                        break
            except Exception as ex:
                print(f"file not found at {target}: {ex}")
                # No file there, ok!
                pass
            else:
                raise Exception("File already exists")

            print(f"copying to {target}")

            size = os.stat(src).st_size

            with open(src, "rb") as infile:
                start = time.time()
                self._dbfs.put_from_file(infile, target, size=size)
                delta = max(1, time.time() - start)

        except:
            self._tell_error(f"{arg}: put failed")

    def _get_to_temp(self, src, prefix=".tmp-", suffix=None, **kwargs):
        try:
            if suffix is None:
                bn = os.path.basename(src)
                try: suffix = bn[bn.rindex("."):]
                except: suffix = ""

            (f, target) = tempfile.mkstemp(prefix=prefix, suffix=suffix, **kwargs)
            out = os.fdopen(f, "wb")
            self._dbfs.get_to_file(src, out)
            out.close()
            return target

        except Exception as ex:
            try: out.close()
            except: pass
            try: os.remove(tmp_fn)
            except: pass
            raise ex

    def do_get(self, arg):
        try:
            src, target = self._parse(arg, ".", min=1)

            if os.path.isdir(target):
                target = os.path.join(target, os.path.basename(src))
            if os.path.exists(target):
                raise Exception("file already exists")

            parent_dir, _ = os.path.split(target)
            self._local_mkdir(parent_dir)

            tmp_target = self._get_to_temp(src, prefix=".transferring-", suffix="", dir=parent_dir)
            os.rename(tmp_target, target)

        except:
            self._tell_error(f"{arg}: unable to retrieve remote file")

    def _get_and_call(self, src, cb):
        target = self._get_to_temp(src)
        try:
            cb(target)
        finally:
            try: os.remove(target)
            except: pass

    def do_rm(self, arg):
        try:
            path, = self._parse(arg, min=1)
            self._dbfs.rm(path)
            print("File removed");
        except:
            self._tell_error(f"{arg}: unable to remove remote file")

    def do_rmdir(self, arg):
        try:
            path, = self._parse(arg, min=1)
            self._dbfs.rm(path, recursive=True)
            print("Dir removed");
        except:
            self._tell_error(f"{arg}: unable to remove remote file")

    def do_find(self, arg):
        try:
            path, = self._parse(arg, ".")
            path = self._dbfs._resolve(path)

            def update_cb(fi, ok, ex):
                print("{:>4} {:>7} {:>19} {}".format(fi.type(),
                                                     self._format_size(fi.size()),
                                                     self._format_time(fi.mtime()),
                                                     fi.relpath(path)))
                if ex:
                    print("# Unable to recurse into {fi.relpath(path)}, {ex}")
                    raise ex

            self._dbfs.find(path, update_cb)

        except:
            self._tell_error(f"{arg}: unable to recursively list remote dir");

    def _rgetput_update_cb(self, path, ok, ex):
        if ok:
            print(f"{path} ok!")
        elif ex is None:
            print(f"{path} FAILED!")
        else:
            print(f"{path} FAILED {ex}!")

    def do_rput(self, arg):
        try:
            src, target = self._parse(arg, None, min=1)
            normalized_src = os.path.normpath(src)
            if target is None:
                if normalized_src == "." or normalized_src == "/":
                    target = "."
                else:
                    target = os.path.basename(normalized_src)

            self._dbfs.rput(src, target, self._rgetput_update_cb)
        except:
            self._tell_error(f"{arg}: rput failed")

    def do_rget(self, arg):
        try:
            src, target = self._parse(arg, None, min=1)
            normalized_src = os.path.normpath(src)
            if target is None:
                if normalized_src == "." or normalized_src == "/":
                    target = "."
                else:
                    target = os.path.basename(normalized_src)

            self._dbfs.rget(src, target, self._rgetput_update_cb)
        except:
            self._tell_error(f"{arg}: rget failed")

    def do_cat(self, arg):
        try:
            src, = self._parse(arg, min=1)
            def cb(fn):
                subprocess.run(["cat", "--", fn])
            self._get_and_call(arg, cb)
        except:
            self._tell_error(f"{arg}: unable to show file")

    def do_more(self, arg, pager=None):
        try:
            src, = self._parse(arg, min=1)
            if pager is None:
                pager = self.cfg("fastdbfs", "pager")
            def cb(fn):
                subprocess.run([pager, "--", fn])
            self._get_and_call(arg, cb)
        except:
            self._tell_error(f"{arg}: unable to show file")

    def do_edit(self, arg, editor=None):
        try:
            src, = self._parse(arg, min=1)
            if editor is None:
                editor = self.cfg("fastdbfs", "editor", default=os.environ.get("EDITOR", "vi"))

            def cb(tmp_fn):
                # in order to avoid race conditions we force the mtime
                # of the temporal file into the past
                the_past = int(time.time() - 2)
                os.utime(tmp_fn, times=(the_past, the_past))
                subprocess.run([editor, "--", tmp_fn])
                stat_after = os.stat(tmp_fn)
                if (stat_after.st_mtime > the_past):
                    with open(tmp_fn, "rb") as infile:
                        self._dbfs.put_from_file(infile, src, size=stat_after.st_size, overwrite=True)
                else:
                    raise Exception(f"File was not modified!")

            self._get_and_call(arg, cb)
        except:
            self._tell_error(f"{arg}: edit failed")

    def do_mg(self, arg):
        self.do_edit(arg, editor="mg")

    def do_vi(self, arg):
        self.do_edit(arg, editor="vi")

    def do_less(self, arg):
        self.do_more(arg, pager="less")

    def do_batcat(self, arg):
        self.do_more(arg, pager="batcat")

    def do_shell(self, arg):
        os.system(arg)

    def do_EOF(self, arg):
        print("\nBye!")
        return True

    def do_exit(self, arg):
        print("Bye!")
        return True

    def _set_prompt(self):
        if self._dbfs:
            self.prompt = self._dbfs.prompt()
        else:
            self.prompt = "*disconnected* "

    def postcmd(self, stop, line):
        self._set_prompt()
        return stop

    def preloop(self):
        if self.cfg("DEFAULT", "token", None) is not None:
            self.do_open("")
        self._set_prompt()

    def _parse(self, arg, *defaults, min=0, max=None):
        args = shlex.split(arg)
        max1 = max if max is not None else min + len(defaults)
        if ((len(args) < min) or
            (max1 >= 0 and len(args) > max1)):
            raise Exception("wrong number of arguments")

        args += defaults[len(args)-min:]

        return args
