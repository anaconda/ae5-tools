### AE5 Command Line Tool

This tool uses AE5's `/api/v2` to do cool things.

Commands implemented: `ae5 <command-group> <command>`

- `project`: `list`, `info`, `download`, `upload`, `delete`
- `revision`: `list`, `info`, `download`
- `session`: `list`, `info`
- `deployment`: `list`, `info`
- `job`: `list`, `info`
- `credentials`: `list`, `add`, `remove`

Lots more to come, including session, deployment, and job controls. The intent is also to make it possible to allow packages to plug into the tool and provide additional commands.

General capabilities:

- Adoption of a standard project identifier format `<owner>/<name>/<id>:<revision>`, with convenient defaults:
	- `<id>` can usually be omitted, leaving `<owner>/<name>:<revision>`
	- `<revision>` can be omitted in most contexts, with the latest revision considered by default; the latest revision can also be specified with `:latest`, Docker-style
	- `<owner>` can be also be omitted, allowing projects to be specified solely by `<name>` or `<id>`. The ambiguity of these choices is resolved by assuming no project will have a name matching the `<id>` format `a[0-9a-f]-[0-9a-f]{32}`. 
- Output formats include terminal-formatted text tables, CSV files, and JSON.
- All tabular output can be filtered by simple field matching, and sorted by columns.
- Hostname, username, and password can be specified as command-line options or as environment variables, to facilitate programmatic use.
- Login sessions are persisted to `~/.ae5/cookies`, so that multiple commands can be issued without having to re-enter passwords.
- A REPL mode provided by [click-repl](https://github.com/click-contrib/click-repl) be entered by typing `ae5` with no positional arguments, enabling multiple commands to be entered in a single session, with autocompletion, inline help, and persistent history.