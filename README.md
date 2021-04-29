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
  
* `put src [dest]`: copy the file to the remote file system.

* `get src [dest]`: copy the remote file to the local filesystem.

* `rm file`: removes the remote file.

* `more file`: shows the contents of the remote file.

* `edit file`: retrieves the remote file, opens it in the configured
  editor and if it is changed, it copies it back.
  
* `!cmd ...`: runs the given command locally.

* `exit`: exits the client.

# Configuration

`fastdbfs` reads its configuration from `~/.config/fastdbfs` and
`~/databrickscfg`.

See the sample files accompaning the program.

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
