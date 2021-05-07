import sys
import cmd
import configparser
import os
import os.path
import posixpath
import traceback
import time
import subprocess
import logging
import progressbar
import tempfile

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

        Changes the remote current directory.
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
        """
        ls [OPTS] [path]

        List the contents of the remote directory.

        The accepted options are as follows:

          -l, --long   Print file properties.

          -h, --human  Print file sizes in a human friendly manner.
        """
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

        Displays the local working directory.
        """

        print(os.getcwd())

    @flag("overwrite", "o")
    @local("src")
    @remote("target", default=".")
    def do_put(self, overwrite, src, target):
        """
        put [OPTS] src [target]

        Copies the given local file to the remote system.

        Supported options are:

          -o, --overwrite  When a file already exists at the target
                           location, it is overwritten.
        """

        try:
            if self._dbfs.filetest_d(target):
                target = os.path.join(target, posixpath.basename(src))
        except: pass

        with progressbar.DataTransferBar() as bar:
            def update_cb(size, bytes_copied):
                bar.max_value=size
                bar.update(bytes_copied)

            self._dbfs.put(src, target,
                           overwrite=overwrite,
                           update_cb=update_cb)

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

        parent_dir = os.path.dirname(target)
        fastdbfs.util.mkdirs(parent_dir)

        with progressbar.DataTransferBar() as bar:
            def update_cb(size, bytes_copied):
                bar.max_value=size
                bar.update(bytes_copied)

            self._dbfs.get(src, target,
                           overwrite=overwrite,
                           update_cb=update_cb)

    def _get_to_temp(self, src, suffix=None, **mkstemp_args):
        if suffix is None:
            bn = posixpath.basename(src)
            try: suffix = bn[bn.rindex("."):]
            except: suffix = ""

        with progressbar.DataTransferBar() as bar:
            def update_cb(size, bytes_copied):
                bar.max_value=size
                bar.update(bytes_copied)
            return self._dbfs.get_to_temp(src, suffix=suffix, **mkstemp_args)

    @flag("recursive", "R")
    @remote("path")
    def do_rm(self, recursive, path):
        """
        rm [OPTS] path

        Removes the remote file or directory.

        Supported options are as follows:

          -R, --recursive Delete files and directories recursively.
        """
        self._dbfs.rm(path, recursive=recursive)

    @flag("overwrite", "o")
    @remote("src")
    @remote("target")
    def do_mv(self, overwrite, src, target):
        """
        mv src target

        Move the file or directory "src" to "target" in the remote
        system.

        Supported options are as follows:

          -o, --overwrite  If a file with the given target name
                           already exists, it is overwritten.
        """

        try:
            if self._dbfs.filetest_d(target):
                target = posixpath.join(target, posixpath.basename(src))
        except: pass

        self._dbfs.mv(src, target, overwrite=overwrite)

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
                       Case insensitive version of "re".
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

        Supported options are:

          -o, --overwrite  Overwrites remote files.
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
    @flag("overwrite", "o")
    @flag("sync", "synchronize")
    @remote("src")
    @local("target", arity="?")
    @_find_predicates
    def do_rget(self, verbose, nowarn, overwrite, sync,
                src, target, external_filter, **predicates):
        """
        rget [OPTS] [RULES] [src [target]]

        Copies the given remote directory to the local system
        recursively.

        The options supported are as follows:

        -v, --verbose    Display the names of the files being copied.
        --nowarn         Do not show warnings.
        -o, --overwrite  Overwrite existing files.
        --sync           Copy only files that have changed.

        In addition to those, "rget" also accepts the same set of
        predicates as "find" for selecting the entries to copy.

        When "sync" mode is enabled, before transferring a file it is
        checked whether a local file already exists at the
        destination, if it is as new as the remote one and if the
        sizes are the same. The download is skipped whan all these
        conditions are true.

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
                            overwrite=overwrite,
                            sync=sync,
                            update_cb=update_cb,
                            filter_cb=self._wrap_external_filter(external_filter),
                            predicates=predicates)

    def _get_and_call(self, src, cb):
        target = self._dbfs.get_to_temp(src)
        try:
            return cb(target)
        finally:
            try: os.remove(target)
            except: pass

    def _get_call_and_put(self, src, cb, new=False, backup=None):

        if backup == "":
            raise Exception("Backup termination can not be the empty string")

        def call_and_put(tmp_fn):
            # in order to avoid race conditions we move the mtime
            # of the temporal file into the past
            the_past = int(time.time() - 2)
            os.utime(tmp_fn, times=(the_past, the_past))

            cb(tmp_fn)

            stat_after = os.stat(tmp_fn)
            if (stat_after.st_mtime > the_past):

                if backup is not None:
                    self._dbfs.mv(src, src+backup, overwrite=True)

                with progressbar.DataTransferBar() as bar:
                    def update_cb(size, bytes_copied):
                        bar.max_value=size
                        bar.update(bytes_copied)
                    self._dbfs.put(tmp_fn, src, overwrite=True,
                                   update_cb=update_cb)

            else:
                raise Exception(f"File was not modified!")
        if new:
            if self._dbfs.filetest_e(src):
                raise Exception("File already exists")
            bn = posixpath.basename(src)
            try: suffix = bn[bn.rindex("."):]
            except: suffix = ""
            (f, tmp_fn) = tempfile.mkstemp(suffix=suffix)
            os.close(f)
            try:
                return call_and_put(tmp_fn)
            finally:
                try: os.remove(tmp_fn)
                except: pass

        return self._get_and_call(src, call_and_put)

    def _get_and_run(self, src, *cmd):
        def cb(fn):
            subprocess.run([*cmd, fn])
        self._get_and_call(src, cb)

    def _get_run_and_put(self, src, *cmd, new=False, backup=None):
        def cb(fn):
            subprocess.run([*cmd, fn], check=True)
        self._get_call_and_put(src, cb, new=new, backup=backup)

    @remote("path")
    def do_cat(self, path):
        """
        cat path

        Prints the contents of the remote file.
        """
        self._get_and_run(path, "cat", "--")

    def _do_show(self, path, pager=None):
        if pager is None:
            pager = self.cfg("fastdbfs", "pager")
        self._get_and_run(path, pager, "--")

    @option("pager")
    @remote("path")
    def do_show(self, pager, path):
        """
        show [OPTS] path

        Display the contents of the remote file using your favorite
        pager.

        The supported options are as follows:

          --pager=PAGER  Picks the pager.
        """
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

    def _do_edit(self, path, editor, new):
        if editor is None:
            editor = self.cfg("fastdbfs", "editor", default=os.environ.get("EDITOR", "vi"))

        self._get_run_and_put(path, editor, "--", new=new)

    @flag("new", "n")
    @option("editor")
    @remote("path")
    def do_edit(self, editor, new, path):
        """
        edit [OPTS] path

        Retrieves the remote file and opens it using your favorite editor.

        Once you closes the editor it copies the file back to the
        remote system.

        The supported options are as follows:

          -n, --new  Creates a new file.
          --editor=EDITOR
                     Picks the editor.
        """
        self._do_edit(path, new=new, editor=editor)

    @flag("new", "n")
    @remote("path")
    def do_mg(self, new, path):
        self._do_edit(path, new=new, editor="mg")

    @flag("new", "n")
    @remote("path")
    def do_vi(self, new, path):
        self._do_edit(path, new=new, editor="vi")

    @option("backup", "i")
    @arg("path")
    @arg("cmd", arity="+")
    def do_filter(self, backup, path, cmd):
        """
        filter [OPTS] path cmd...

        Retrieves the remote file and uses the given command to
        process it. The output of the filter is saved as the new file.

        Supported options are:

          -i, --backup=suffix  The contents of the old file are saved
                               to a new file with the given suffix
                               attached to its name.

        Examples:
          filter data.json jq .
          filter -i.bak example.txt -- perl -ne 'print lc $_'
        """
        def cb(in_fn):
            (f, out_fn) = tempfile.mkstemp()
            try:
                with os.fdopen(f, "wb") as outfile:
                    with open(in_fn, "rb") as infile:
                        subprocess.run(cmd, stdin=infile, stdout=outfile, check=True)
                os.remove(in_fn)
                os.rename(out_fn, in_fn)
            finally:
                try: os.remove(out_fn)
                except: pass
        self._get_call_and_put(path, cb, backup=backup)

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
        logging.basicConfig(force=1, **logcfg)

        if "token" in self._cfg["DEFAULT"]:
            self._do_open("DEFAULT")
        self._set_prompt()
