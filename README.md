# Introduction

`fastdbfs` is an interactive command line client for Databricks DBFS
API with the aim of being fast, friendly and feature rich.

`fastdbfs` is still in alpha state. Anything may change at this
point. You should expect bugs and even data corruption or data
lost at times.

Comments, bug reports, ideas and patches or pull requests are very
welcome.

# Commands

* `open [profile]`: sets the Databricks profile used for
  communicating. By default it uses `DEFAULT`.

* `cd [directory]`: sets the remote directory.

* `lcd [directory]`: sets the local directory.

* `lpwd`: shows the current local directory

* `ls [directory]`: list the contents of the given directory (or the
  current one if the argument is omitted).

* `find [directory]`: list the contents of the given directory
  recursively.

* `put src [dest]`: copies the file to the remote file system.

* `get src [dest]`: copies the remote file to the local filesystem.

* `rput src [dest]`: copies the local directory recursively.

* `rget src [dest]`: copies the remote directory recursively.

* `rmdir dir`: removes the remote directory recursively.

* `rm file`: removes the remote file.

* `mkdir dir`: creates the directory (and any non-existent parents).

* `mkcd dir`: creates the directory and sets it as the working
  directory.

* `cat file`: prints the contents of the remote file.

* `more file`: shows the contents of the remote file.

* `edit file`: retrieves the remote file, opens it in the configured
  editor and if it is changed, it copies it back.

* `!cmd ...`: runs the given command locally.

* `exit`: exits the client.

# Configuration

`fastdbfs` reads its configuration from `~/.config/fastdbfs` and
`~/databrickscfg`.

See the sample files accompaning the program.

# Limitations

* Only Linux/UNIX is currently supported. Patches welcome!!!

* The DBFS API has some limitations that `fastdbfs` can not overcome:

    - Directory listings timeout after 1min.

    - The API has a throttling mechanism that slows down operations that
      require a high number of calls (i.e. find, rput, rget).

    - The methods provided for uploading data are too simplistic. They can
      not be parallelized and in some edge cases transfers may become
      corrupted (`fastdbfs` tries to protect against that).

# TODO

* Add more commands: mirror, mget, mput, glob, etc.

* Improve command line parsing allowing for command flags, pipes,
  redirections, etc.

* Autocomplete.

* Make history persistent between sessions.

* Allow passing commands when the program is launched (for instance,
  setting the default profile).

* Help system.

* Better usability.

* Convert it into a Python module.

* Break the code in several modules and clean it.

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
