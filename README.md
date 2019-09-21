## AE5 Command Line Tool

[![Travis Status](https://travis-ci.com/Anaconda-Platform/ae5-tools.svg?branch=master)](https://travis-ci.com/Anaconda-Platform/ae5-tools) &nbsp; [![Anaconda-Server Badge](https://anaconda.org/ae5-admin/ae5-tools/badges/latest_release_date.svg)](https://anaconda.org/ae5-admin/ae5-tools) &nbsp; [![Anaconda-Server Badge](https://anaconda.org/ae5-admin/ae5-tools/badges/version.svg)](https://anaconda.org/ae5-admin/ae5-tools)

This tool uses AE5's internal APIs to do cool things, including:

- obtaining information about projects, sessions, deployments, jobs, and runs
- starting and stopping sessions and deployments
- uploading and downloading projects

More to come. The intent is also to make it possible to allow packages to plug into the tool and provide additional commands.

There is already a fair amount of inline help, so type `ae5 --help` to get started.

### Installation

The prefferred and supported way to install `ae5-tools` is with `conda`. The latest version of the tool
is always available on the [`ae5-admin` channel on anaconda.org](https://anaconda.org/ae5-admin/ae5-tools).
To install this package, you can use this command:
```
conda install -c https://conda.anaconda.org/ae5-admin ae5-tools
```
If you want the _unsupported, bleeding-edge, development_ version, use this command:
```
conda install -c https://conda.anaconda.org/ae5-admin/label/dev ae5-tools
```
The package has the following particular dependencies:
- Python 3.6 or later.
- [Click](https://click.palletsprojects.com/en/7.x/) 7.0 or later
- [click-repl](https://github.com/click-contrib/click-repl), which is in conda-forge, but also provided
  in the `ae5-admin` channel for convenience.

### General capabilities

- Adoption of a standard project identifier format `<owner>/<name>/<id>:<revision>`, with convenient defaults:
    - `<id>` can usually be omitted, leaving `<owner>/<name>:<revision>`
    - `<revision>` can be omitted in most contexts, with the latest revision considered by default; the latest revision can also be specified with `:latest`, Docker-style
    - `<owner>` can be also be omitted, allowing projects to be specified solely by `<name>` or `<id>`. The ambiguity of these choices is resolved by assuming no project will have a name matching the `<id>` format `a[0-3]-[0-9a-f]{32}`. 
- Output formats include terminal-formatted text tables, CSV files, and JSON.
- All tabular output can be filtered by simple field matching, and sorted by columns.
- Hostname, username, and password can be specified as command-line options or as environment variables, to facilitate programmatic use.
- Login sessions are persisted to `~/.ae5`, so that multiple commands can be issued without having to re-enter passwords.
- Keycloak impersonation allows administrators to run commands on behalf of them.
- A REPL mode provided by [click-repl](https://github.com/click-contrib/click-repl) be entered by typing `ae5` with no positional arguments, enabling multiple commands to be entered in a single session, with autocompletion, inline help, and persistent history.

### Command Tree

- Composite commands:
    - `account`: `list`
    - `deployment`: `info`, `list`, `logs`, `open`, `patch`, `restart`, `start`, `stop`, `token`
      - `collaborator`: `list`, `info`, `add`, `remove`
    - `editor`: `info`, `list`
    - `endpoint`: `info`, `list`
    - `job`: `create`, `delete`, `info`, `list`, `patch`, `run`, `runs`, `unpause`
    - `project`: `activity`, `delete`, `deploy`, `deployments`, `download`, `info`, `jobs`,
      `list`, `patch`, `run`, `runs`, `schedule`, `sessions`, `status`, `upload`
      - `collaborator`: `add`, `info`, `list`, `remove`
      - `revision`: `list`, `info`, `download`
    - `sample`: `info`, `list`
    - `run`: `delete`, `info`, `list`, `log`, `stop`
    - `session`: `info`, `list`, `open`, `start`, `stop`
    - `user`: `info`, `list`
- Simple commands: `call`, `login`, `logout`
- Login options: `--hostname`, `--username`, `--admin-username`, `--admin-hostname`, `--impersonate`
- Output format options: `--format`, `--filter`, `--columns`, `--sort`, `--width`, `--wide`, `--no-header`
- Help options: `--help-format`, `--help-filter`, `--help-login`, `--help`

### Support

We use this tool internally, so we're grateful for your feedback and look forward to continuously
improving it! To submit a bug report or feature request, please use our GitHub
[issue tracker](https://github.com/Anaconda-Platform/ae5-tools/issues). We will address these issues
as time permits. Rapid response support and prioritized feature development can be provided as part
of a paid engagement with our Services team. Please contact your Customer Success Manager for details.
