# Introduction

`fastdbfs` is an interactive command line client for Databricks DBFS
API with the aim of being fast, friendly and feature rich.

`fastdbfs` is still in alpha state. Anything may change at this
point. You should expect bugs and even data corruption or data
lost at times.

Comments, bug reports, ideas and patches or pull requests are very
welcome.

# Installation

As development of `fastdbfs` progresses, from time to time and at
points where it is considered to be more or less stable, releases on
PyPi are done.

Those versions can be installed using `pip` as follows:

    pip install fastdbfs

But at this early development stage, getting `fastdbfs` directly from
GitHub is still probably a better idea, even if it may be broken at
times, you will enjoy newer features.

You can do it as follows:

    git clone https://github.com/salva/fastdbfs.git
    cd fastdbfs
    python setup.py install --user


# Usage

Once the program is installed, just invoke it from the command line as
`fastdbfs`.

I don't recomend at this point the usage of the python modules
directly as the interfaces are not stable yet.

## Configuration

`fastdbfs` reads its configuration from `~/.config/fastdbfs` and
`~/.databrickscfg` (on windows that translates to something like
`C:\Users\migueldcs\.config\fastdbfs` and
`C:\Users\migueldcs\.databrickscfg` respectively).

See the sample configuration file `fastdbfs-sample-config` distributed
with the program.

## Commands

Once `fastdbfs` is launched, a prompt appears and the following
commands can be used:

### `open [profile]`

Sets the active Databricks profile used for communicating.

By default it uses `DEFAULT`.

### `cd [directory]`

Changes the remote current directory.

### `lcd [directory]`

Sets the local working directory.

### `lpwd`

Shows the working local directory

### `ls [OPTS] [directory]`:

List the contents of the remote directory.

The supported options are as follows:

* `-l`, `--long`: Include file properties size and modification time.

* `-h`, `--human`: Print file sizes in a human friendly manner.

### `find [OPTS] [RULES] [directory]`

List files recursively according to a set of rules.

Supported options are as follows:

* `--nowarn`: Do not report errors happening while walking the file
  system tree.

* `-l`, `--ls`: Display file and directory properties.

* `-h`, `--human`: Show sizes like `1K`, `210M`, `4G`, etc.

  In addition to those, find also supports the following set of
  options for selectively picking the files that should be
  displayed:

* `--min-size=SIZE`, `--max-size=SIZE`: Picks files according to their
    size (this rule is ignored for directories).

* `--newer-than=date`, `--older-than=date`: Picks entries according to
  their modification time.

* `--name=GLOB_PATTERN`: Picks only files and directories whose
  basename matches the given glob pattern.

* `--iname=GLOB_PATTERN`: Case insensitive version of `name`.

* `--re=REGULAR_EXPRESSION`: Picks only entries whose basename matches
  the given regular expression.

* `--ire=REGULAR_EXPRESSION`: Case insensitive version of `re`.

* `--wholere=REGULAR_EXPRESSION`: Picks file names whose relative path
    matches the given regular expression.

* `--iwholere=REGULAR_EXPRESSION`: Case insensitive version of
  `wholere`.

* `--external-filter=CMD`: Filters entries using an external command.

Also, any of the rules above can be negated preceding it by
`--exclude`, for instance `--exclude-iname=*.jpeg`

The rules above never cut the file system traversal. So for instance,
a rule discarding some subdirectory, doesn't preclude that
subdirectory for being traversed and its child entries picked.

The basename is the last component of a patch. Relative paths are
considered relative to the root directory passed as an argument.

Example:

    find --ls --newer-than=yesterday --max-size=100K --iname=*.jpg

### `put src [dest]`

Copies the given local file to the remote system.

Supported options are:

* `-o`, `--overwrite`: When a file already exists at the target
  location, it is overwritten.

### `get [OPTS] src [dest]`

Copies the given remote file to the local system.

Supported options are as follows:

* `-o`, `--overwrite`: When a file already exists at the target
  location, it is overwritten.

### `rget [OPTS] [RULES] [src [target]]`

Copies the given remote directory to the local system
recursively.

The options supported are as follows:

* `-v`, `--verbose`: Display the names of the files being copied.

* `--nowarn`: Do not show warnings.

* `-o`, `--overwrite`: Overwrite existing files.

* `--sync`: Copy only files that have changed.

  Before transferring a file it is checked whether a local file
  already exists at the destination, if it is as new as the remote one
  and if the sizes are the same. The download is skipped whan all
  these conditions are true.

In addition to those, `rget` also accepts the same set of predicates
as `find` for selecting the entries to copy.

Examples:

    rget --iname *.png --exclude-iwholere=/temp/ . /tmp/pngs

### `rput [OPTS] [src [dest]]`

Copies the given local directory to the remote system
recursively.

Supported options are:

* `-o`, `--overwrite`: Overwrites any previously existent remote file.

### `rm [OPTS] path`

Delete the remote file or directory.

Supported options are as follows:

* `-R`, `--recursive`: Delete files and directories recursively.

### `mkdir dir`

Creates the directory (and any required parents).

### `mkcd dir`

Creates the directory and sets it as the working directory.

### `cat file`

Prints the contents of the remote file.

### `show [OPTS] file`

Shows the contents of the remote file using the configured pager (for
instance, `more`).

The supported options are as follows:

* `--pager=PAGER`: Picks the pager for displaying the file contents.

The pager can be configured adding an entry `pager` inside the
`fastdbfs` section in the configuration file. `less` is used by
default.

The commands `batcat`, `less` and `more` are shortcuts that use the
corresponding pager.

### `edit [OPTS] file`

Retrieves the remote file and opens it using your favorite editor.

Once the editor is closed, the file is copied back to the remote
system

The supported options are as follows:

* `-n`, `--new`: Creates a new file.

* `--editor=EDITOR`: Picks the editor.

By default `fastdbfs` picks the editor from the environment variable
`EDITOR`. It can also be customized in the configuration file creating
an `editor` entry inside the `fastdbfs` section.

The commands `vi` and `mg` are shortcuts for `edit` that will use the
corresponding editors.

### `!cmd ...`

Runs the given command locally.

* `exit`: exits the client.

## External filters

*TODO*

# Limitations

* Development is primarily done on Linux and only from time to time is
  `fastdbfs` tested on Windows. Don't hesitate to report bugs related
  to this.

* The DBFS API has some limitations that `fastdbfs` can not overcome:

    - Directory listings timeout after 1min.

    - The API has a throttling mechanism that slows down operations that
      require a high number of calls (i.e. find, rput, rget).

    - The methods provided for uploading data are too simplistic. They can
      not be parallelized and in some edge cases transfers may become
      corrupted (`fastdbfs` tries to protect against that).

    - The metadata available is limited to file size and modification
      time.


* Glob expression checking is done using python `fnmatch` module that
  only supports a very small subset of patterns. Specifically, it
  lacks support for alternations as in `*.{jpeg,jpg,png}`.

# TODO

* Add more commands: mget, mput, glob, etc.

* Improve command line parsing allowing for command flags, pipes,
  redirections, etc.

* Autocomplete.

* Make history persistent between sessions.

* Allow passing commands when the program is launched (for instance,
  setting the default profile).

* Catch C-c during long tasks and perform an orderly cleanup
  (i.e. remove temporary files).

* Improve code quality.

# Development and support

The source code for this program is available from
https://github.com/salva/fastdbfs


# Copyright

Copyright (C) 2021 Salvador Fandiño García (sfandino@yahoo.com)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
