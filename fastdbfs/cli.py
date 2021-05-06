import sys
import cmd
import configparser
import os
import os.path
import posixpath
import traceback
import time
import shlex
import pathlib
import subprocess
import tempfile
import logging
import progressbar

from fastdbfs.dbfs import DBFS, Disconnected
from fastdbfs.cmdline import option, flag, arg, remote, local, argless, chain
from fastdbfs.format import Table, format_human_size, format_time
import fastdbfs.util

_find_predicates=chain(option("min-size", cast="size"),
                       option("max-size", cast="size"),
                       option("max-depth", cast="int"),
                       option("min-depth", cast="int"),
                       option("iname", cast="glob", cast_args={"case_insensitive": True}),
                       option("name", cast="glob"),
                       option("re", "regexp", cast="re"),
                       option("ire", "iregexp", cast="re", cast_args={"case_insensitive": True}),
                       option("wholere", "whole-regexp", cast="re"),
                       option("iwholere", "iwhole-regexp",
                              cast="re", cast_args={"case_insensitive": True}),
                       option("newer-than", "newer", cast="date>"),
                       option("older-than", "older", cast="date<"),
                       option("exclude-iname", cast="glob", cast_args={"case_insensitive": True}),
                       option("exclude-name", cast="glob"),
                       option("exclude-re", "exclude-regexp", cast="re"),
                       option("exclure-ire", "exclude-iregexp",
                              cast="re", cast_args={"case_insensitive": True}),
                       option("exclude-wholere", "exclude-whole-regexp", cast="re"),
                       option("exclude-iwholere", "exclude-iwhole-regexp",
                              cast="re", cast_args={"case_insensitive": True}),
                       option("external-filter", "ext-filter"))

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
        self._cfg["logging"] = {}

        fns = [os.path.expanduser(fn) for fn in ("~/.databrickscfg", "~/.fastdbfs", "~/.config/fastdbfs")]
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

    def _do_ls(self, path, long, human):
        """
        ls [OPTS] [path]

        List the contents of the remote directory.

        The accepted options are as follows:

          -l, --long   Print file properties.

          -h, --human  Print file sizes in a human friendly manner.
        """

        if long:
            size_format = "human_size" if human else "size"
            table = Table("right_text", size_format, "time", "text")
        else:
            table = Table(None, None, None, "text")
        for fi in self._dbfs.ls(path):
            table.append(fi.type(), fi.size(), fi.mtime(), fi.basename())
        table.print()

    @flag("long", "l")
    @flag("human", "h")
    @remote("path", default=".")
    def do_ls(self, path, long, human):
        return self._do_ls(path, long, human)

    @flag("human", "h")
    @remote("path", default=".")
    def do_ll(self, path, human):
        """
        ll [OPTS] [path]

        ll is an alias for "ls -l".
        """
        return self._do_ls(path, True, human)

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

    @flag("overwrite", "o")
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

        try:
            fi = self._dbfs.get_status(target)
            if fi.is_dir():
                target = os.path.join(target, os.path.basename(src))
        except: pass

        with progressbar.DataTransferBar() as bar:
            def update_cb(size, bytes_copied):
                bar.max_size=size
                bar.update(bytes_copied)

            self._dbfs.put(src, target, overwrite=overwrite, update_cb=update_cb)

    def _get_to_temp(self, src,  update_cb=None, prefix=".tmp-", suffix=None, **kwargs):
        try:
            if suffix is None:
                bn = posixpath.basename(src)
                try: suffix = bn[bn.rindex("."):]
                except: suffix = ""

            (f, target) = tempfile.mkstemp(prefix=prefix, suffix=suffix, **kwargs)
            out = os.fdopen(f, "wb")
            self._dbfs.get_to_file(src, out, update_cb=update_cb)
            out.close()
            return target

        except Exception as ex:
            try: out.close()
            except: pass
            try: os.remove(tmp_fn)
            except: pass
            raise ex

    @flag("overwrite", "o")
    @remote("src")
    @local("target", arity="?", default=".")
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
        if os.path.exists(target) and not overwrite:
            raise Exception("Target file already exists")

        parent_dir = os.path.dirname(target)
        fastdbfs.util.mkdirs(parent_dir)

        with progressbar.DataTransferBar() as bar:
            def update_cb(size, bytes_copied):
                bar.max_size=size
                bar.update(bytes_copied)

            tmp_target = self._get_to_temp(src, update_cb=update_cb,
                                           prefix=".transferring-", suffix="", dir=parent_dir)
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

    def _wrap_external_filter(self, cmd):
        if cmd is None:
            return None

        def filter(fis):
            data = { relpath: fi.to_data() for relpath, fi in fis.items() }
            return fastdbfs.util.call_external_processor_json(cmd, data)
        return filter

    @flag("quiet", "nowarn", "q")
    @flag("long", "ls", "l")
    @flag("human", "h")
    @_find_predicates
    @remote("path", default=".")
    def do_find(self, path, quiet, long, human, external_filter, **predicates):
        """
        find [OPTS] [RULES] [path]

        List files recursively according to a set of rules.

        Supported options are as follows:

          --nowarn     Do not report errors happening while
                       walking the file system tree.
          -l, --ls     Display file and directory properties.
          -h, --human  Show sizes like 1K, 210M, 4G, etc.

        In addition to those, find also supports the following set of
        options for selectively picking the files that should be
        displayed:

          --min-size=SIZE, --max-size=SIZE
                       Picks files according to their size (this rule
                       is ignored for directories).
          --newer-than=date, --older-than=date
                       Picks entries according to their modification time.
          --name=GLOB_PATTERN
                       Picks only files and directories whose basename
                       matches the given glob pattern.
          --iname=GLOB_PATTERN
                       Case insensitive version of "name".
          --re=REGULAR_EXPRESSION
                       Picks only entries whose basename matches the
                       given regular expression.
          --ire=REGULAR_EXPRESSION
                       Case insensitive version of "ire".
          --wholere=REGULAR_EXPRESSION
                       Picks file names whose relative path matches the
                       given regular expression.
          --iwholere=REGULAR_EXPRESSION
                       Case insensitive version of "wholere".
          --external-filter=CMD
                       Filters entries using an external command.

        Also, any of the rules above can be negated preceding it by
        "--exclude", for instance "--exclude-iname=*.jpeg"

        The rules above never cut the file system traversal. So for
        instance, a rule discarding some subdirectory, doesn't
        preclude that subdirectory for being traversed and its child
        entries picked.

        The basename is the last component of a patch. Relative paths
        are considered relative to the root directory passed as an
        argument.

        Example:

          find --ls --newer-than=yesterday --max-size=100K --iname=*.jpg
        """

        with progressbar.ProgressBar(redirect_stdout=True, redirect_stderr=True) as bar:
            def update_cb(entry, max_entries, done):
                bar.max_value = max_entries
                bar.update(done)
                if entry.good:
                    fi = entry.fi
                    relpath = fi.relpath(self._dbfs.cwd, path)
                    if long:
                        size = fi.size()
                        if human:
                            size= format_human_size(size)
                        mtime = format_time(fi.mtime())
                        print("{:>4} {:>12} {:>19} {}".format(fi.type(), size, mtime, relpath))
                    else:
                        print(relpath)
                if entry.ex and not quiet:
                    print("# Unable to recurse into {relpath}, {entry.ex}", file=sys.stderr)

            self._dbfs.find(path, update_cb,
                            filter_cb = self._wrap_external_filter(external_filter),
                            predicates = predicates)

    def _rput_update_cb(self, path, ok, ex):
        if ok:
            print(f"{path} ok!")
        elif ex is None:
            print(f"{path} FAILED!")
        else:
            print(f"{path} FAILED {ex}!")

    @flag("overwrite", "o")
    @local("src")
    @remote("target", arity="?")
    def do_rput(self, overwrite, src, target):
        """
        rput [src [target]]

        Copies the given local directory to the remote system
        recursively.
        """
        if target is None:
            normalized_src = os.path.normpath(src)
            # FIXME: this normalization is not gona work for Windows
            if normalized_src == "." or normalized_src == "/":
                target = "."
            else:
                target = os.path.basename(normalized_src)
        self._dbfs.rput(src, target, overwrite=overwrite, update_cb=self._rput_update_cb)

    @flag("verbose", "v")
    @flag("nowarn", "quiet", "q")
    @remote("src")
    @local("target", arity="?")
    @_find_predicates
    def do_rget(self, verbose, nowarn, src, target, external_filter, **predicates):
        """
        rget [OPTS] [RULES] [src [target]]

        Copies the given remote directory to the local system
        recursively.

        The options supported are as follows:

        -v, --verbose  Display the names of the files being copied.
        --nowarn       Do not show warnings.

        In addition to those, "rget" also accepts the same set of
        predicates as "find" for selecting the entries to copy.

        Example:
          rget --iname *.png --exclude-iwholere=/temp/ . /tmp/pngs
        """

        if verbose and nowarn:
            raise Exception("verbose and nowarn can not be used together")

        if target is None:
            # FIXME: Windows again
            normalized_src = os.path.normpath(src)
            if normalized_src == "." or normalized_src == "/":
                target = "."
            else:
                target = os.path.basename(normalized_src)

        with progressbar.ProgressBar(redirect_stderr=True) as bar:
            def update_cb(entry, max_entries, done):
                if not nowarn:
                    relpath = entry.fi.relpath(self._dbfs.cwd, src)
                    if entry.ex:
                        print(f"{relpath}: FAILED. {entry.ex}", file=sys.stderr)
                    elif entry.good and verbose:
                        print(f"{relpath}: copied.", file=sys.stderr)
                bar.max_value = max_entries
                bar.update(done)
            self._dbfs.rget(src, target,
                            update_cb,
                            filter_cb = self._wrap_external_filter(external_filter),
                            predicates = predicates)

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
        print(f"{msg}: {type(ex).__name__} - {ex}")
        logging.debug("Stack trace", exc_info=True)

    def onecmd(self, line):
        try:
            return super().onecmd(line)
        except Exception as ex:
            self._tell_error("Operation failed")
            return False

    def preloop(self):
        # DEFAULTS contaminates everything, so we have to explicitly
        # estate the fields that we want passed to
        # logging.basicConfig().
        section = self._cfg["logging"]
        logcfg = { k: section[k]
                   for k in ("filename", "filemode", "format", "datefmt",
                             "style", "level", "encoding", "errors")
                   if k in section }

        print(f"logcfg: {logcfg}")

        logging.basicConfig(force=1, **logcfg)

        if "token" in self._cfg["DEFAULT"]:
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
