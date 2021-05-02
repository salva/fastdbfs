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
import logging

from fastdbfs.dbfs import DBFS, Disconnected
from fastdbfs.cmdline import option, flag, arg, remote, local, argless
from fastdbfs.format import Table

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
            "error_delay_increment": 10,
            "log_level": "WARNING"
        }

        home = os.path.expanduser("~")
        fns = [os.path.join(home, fn) for fn in (".databrickscfg", ".fastdbfs", ".config/fastdbfs")]
        self._cfg.read(fns)

    def _do_open(self, profile):
        dbfs = DBFS(profile,
                    host = self.cfg(profile, "host"),
                    cluster_id = self.cfg(profile, "cluster_id"),
                    token = self.cfg(profile, "token"),
                    chunk_size = self.cfg_int("fastdbfs", "chunk_size"),
                    workers = self.cfg_int("fastdbfs", "workers"),
                    max_retries = self.cfg_int("fastdbfs", "max_retries"))

        dbfs.check()
        self._dbfs = dbfs

    @arg("profile", default="DEFAULT")
    def do_open(self, profile):
        """
        open [profile]

        Sets the active Databricks profile.
        """

        print(f"calling _do_open({profile}")
        self._do_open(profile)

    @remote("path")
    def do_cd(self, path):
        """
        cd [path]

        Change the remote current directory.
        """

        self._dbfs.cd(path)

    def do_mkcd(self, arg):
        """
        mkcd [path]

        Creates the given directory and then sets it as the current directory.
        """

        self.do_mkdir(arg)
        self.do_cd(arg)

    @remote("path")
    def do_mkdir(self, path):
        """
        mkdir path

        Creates the remote path.
        """

        self._dbfs.mkdir(path)

    @flag("human", "h")
    @flag("long", "l")
    @remote("path", default=".")
    def do_ls(self, path, long, human):
        """
ls [OPTS] [path]

        List the contents of the remote directory.

        The accepted options are as follows:

          -l, --long   Print file properties.

          -h, --human Print file sizes in a human friendly manner.
        """

        if long:
            size_format = "human_size" if human else "size"
            table = Table("right_text", size_format, "time", "text")
        else:
            table = Table(None, None, None, "text")
        for fi in self._dbfs.ls(path):
            table.append(fi.type(), fi.size(), fi.mtime(), fi.basename())
        table.print()

    @local("path", default="~")
    def do_lcd(self, path):
        """
        lcd [path]

        Changes the local working directory.
        """

        os.chdir(path)


    @argless()
    def do_lpwd(self):
        """
        lpwd

        Displays the local current directory.
        """

        print(os.getcwd())

    def _local_mkdir(self, path):
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)

    @flag("overwrite")
    @local("src")
    @remote("target", arity="?")
    def do_put(self, overwrite, src, target):
        """
        put [OPTS] src [target]

        Copies the given local file to the remote system.

        Supported options are:

          -o, --overwrite  When a file already exists at the target
                           location, it is overwritten.
        """

        # TODO: implement overwrite
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
            self._dbfs.put_from_file(infile, target, size=size)

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

    @flag("overwrite")
    @remote("src")
    @local("target", arity="?")
    def do_get(self, overwrite, src, target):
        """
        get [OPTS] src [target]

        Copies the given remote file to the local system.

        Supported options are as follows:

          -o, --overwrite  When a file already exists at the target
                           location, it is overwritten.
        """

        if os.path.isdir(target):
            target = os.path.join(target, os.path.basename(src))
        if os.path.exists(target):
            raise Exception("Target file already exists")

        parent_dir, _ = os.path.split(target)
        self._local_mkdir(parent_dir)

        tmp_target = self._get_to_temp(src, prefix=".transferring-", suffix="", dir=parent_dir)
        os.rename(tmp_target, target)

    def _get_and_call(self, src, cb):
        target = self._get_to_temp(src)
        try:
            cb(target)
        finally:
            try: os.remove(target)
            except: pass

    @flag("recursive", "R")
    @remote("path")
    def do_rm(self, recursive, path):
        """
        rm [OPTS] path

        Supported options are as follows:

          -R, --recursive Delete files and directories recursively.
        """
        self._dbfs.rm(path, recursive=recursive)

    @remote("path")
    def do_find(self, path):
        """
        find [path]

        List files recursively.
        """

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

    def _rgetput_update_cb(self, path, ok, ex):
        if ok:
            print(f"{path} ok!")
        elif ex is None:
            print(f"{path} FAILED!")
        else:
            print(f"{path} FAILED {ex}!")

    @local("src")
    @remote("target", arity="?")
    def do_rput(self, arg):
        """
        rput [src [target]]

        Copies the given local directory to the remote system
        recursively.
        """
        if target is None:
            normalized_src = os.path.normpath(src)
            if normalized_src == "." or normalized_src == "/":
                target = "."
            else:
                target = os.path.basename(normalized_src)
        self._dbfs.rput(src, target, self._rgetput_update_cb)

    @remote("src")
    @local("target", arity="?")
    def do_rget(self, arg):
        """
        rget [src [target]]

        Copies the given remote directory to the local system
        recursively.
        """

        if target is None:
            normalized_src = os.path.normpath(src)
            if normalized_src == "." or normalized_src == "/":
                target = "."
            else:
                target = os.path.basename(normalized_src)
        self._dbfs.rget(src, target, self._rgetput_update_cb)

    @remote("path")
    def do_cat(self, path):
        """
        cat path

        Prints the contents of the remote file.
        """
        def cb(fn):
            subprocess.run(["cat", "--", fn])
        self._get_and_call(path, cb)

    def _do_show(self, path, pager=None):
        if pager is None:
            pager = self.cfg("fastdbfs", "pager")
        def cb(fn):
            subprocess.run([pager, "--", fn])
        # print(f"pager: {pager}, cb: {cb}")
        self._get_and_call(arg, cb)

    @option("pager")
    @remote("path")
    def do_show(self, pager, path):
        """
        show path

        Display the contents of the remote file using your favorite
        pager.
        """

        # print(f"pager: {pager}, path: {path}")
        self._do_show(path, pager=pager)

    @remote("path")
    def do_more(self, path):
        self._do_show(path, pager="more")

    @remote("path")
    def do_less(self, path):
        self._do_show(path, pager="less")

    @remote("path")
    def do_batcat(self, path):
        self._do_show(path, pager="batcat")

    def _do_edit(self, path, editor):
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

        self._get_and_call(path, cb)

    @option("editor")
    @remote("path")
    def do_edit(self, editor, path):
        """
        edit path

        Retrieves the remote file and opens it using your favorite editor.

        Once you closes the editor it copies the file back to the
        remote system.
        """
        self._do_edit(path, editor)

    @remote("path")
    def do_mg(self, arg):
        self._do_edit(path, editor="mg")

    @remote("path")
    def do_vi(self, arg):
        self._do_edit(path, editor="vi")

    def do_shell(self, arg):
        """
        !cmd args...

        Runs the given command locally.
        """
        os.system(arg)

    @argless()
    def do_EOF(self):
        print("\nBye!")
        return True

    @argless()
    def do_exit(self):
        """
        exit

        Exits the program.
        """

        print("Bye!")
        return True

    @argless()
    def do_q(self):
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

    def _tell_error(self, msg):
        _, ex, _ = sys.exc_info()
        print(f"{msg}: {ex}")
        logging.debug("Stack trace", exc_info=True)

    def onecmd(self, line):
        try:
            return super().onecmd(line)
        except Exception as ex:
            self._tell_error("Operation failed")
            return False

    def preloop(self):
        logging.getLogger(None).setLevel(self.cfg("fastdbfs", "log_level"))

        if self.cfg("DEFAULT", "token", None) is not None:
            self._do_open("DEFAULT")
        self._set_prompt()

    def _parse(self, arg, *defaults, min=0, max=None):
        args = shlex.split(arg)
        max1 = max if max is not None else min + len(defaults)
        if ((len(args) < min) or
            (max1 >= 0 and len(args) > max1)):
            raise Exception("wrong number of arguments")

        args += defaults[len(args)-min:]

        return args
