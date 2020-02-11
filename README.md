## AE5 Command Line Tool

[![Travis Status](https://travis-ci.com/Anaconda-Platform/ae5-tools.svg?branch=master)](https://travis-ci.com/Anaconda-Platform/ae5-tools) &nbsp; [![Anaconda-Server Badge](https://anaconda.org/ae5-admin/ae5-tools/badges/latest_release_date.svg)](https://anaconda.org/ae5-admin/ae5-tools) &nbsp; [![Anaconda-Server Badge](https://anaconda.org/ae5-admin/ae5-tools/badges/version.svg)](https://anaconda.org/ae5-admin/ae5-tools)

This tool uses AE5's internal APIs to do cool things, including:

- obtaining information about projects, sessions, deployments, jobs, and runs
- starting and stopping sessions and deployments
- uploading and downloading projects

More to come. The intent is also to make it possible to allow packages to plug into the tool and provide additional commands.

There is already a fair amount of inline help, so type `ae5 --help` to get started.

### Installation

The preferred and supported way to install `ae5-tools` is with `conda`. The latest version of the tool
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
    - `session`: `branches`, `changes`, `info`, `list`, `open`, `start`, `stop`
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

### BETA: Kubernetes information support

This version of ae5-tools includes the ability to use the Kubernetes
API to obtain additional information about sessions, deployments, and
job runs, including live resource usage metrics.

- `pod list`
- `node list`
- `session list --k8s`, `session info --k8s`
- `deployment list --k8s`, `deployment info --k8s`

Future updates to this support will add the ability to access the logs
for a session or deployment as well.

To facilitate this, a custom Kubernetes client has been developed to
query the Kubernetes API and deliver a safe, filtered version of the
output to the end user. This client can be utilized in one of two ways:

##### Using SSH

By adding the `--k8s-endpoint=ssh:<username>` option
to any of the above calls, where `<username>` is the name of a user
on the master node, `ae5-tools` will build a temporary SSH tunnel
to the node and perform the client calls. This user must meet two
criteria:
- The user must have _passwordless_ access to the master node
  via the named user (e.g., with an SSH key). That is,
  `ssh <username>@<hostname>` must succeed without requiring
  the entry of a password.
- This user on the master node must be able to execute
  `kubectl` commands with no additional configuration.
The advantage of this approach is that no installation is required.
However, the `--k8s-endpoint` option must be supplied with every
command, or set in the `AE5_K8S_ENDPOINT` environment variable.
And it will only enable support for any user with access to the
master node. The deployment approach, described below, will enable
operation for all `ae5-tools` users.

##### Using the `k8s` deployment

We have constructed a standard AE5 REST API deployment that, when
installed, allows any user of `ae5-tools` to complete the necessary
queries. By default, `ae5-tools` expects this to live at the `k8s`
endpoint. When installed at this location, its presence will
automatically be detected by `ae5-tools`, enabling the use of the
Kubernetes-related commands.
   
The installation method will be improved in future
releases, but for now, it can be installed as follows.
1. First, obtain and install a valid Kubernetes API bearer token.
   1. Log into the master node, into an account with valid `kubectl` access.
   2. Execute this command to retrieve a valid bearer token:
      ```
      echo $(kubectl get secret $(kubectl get serviceaccount default -o jsonpath='{.secrets[0].name}') -o jsonpath='{.data.token}' | base64 --decode)
      ```
      Copy this output into your clipboard or to a safe location.
      This should be a long (approx. 840-character), single-line,
      alphanumeric string.
   3. Now log into AE with your web browser, follow
      [these instructions](https://enterprise-docs.anaconda.com/en/latest/data-science-workflows/user-settings.html#storing-secrets)
      to create a secret in your Anaconda Enterprise account. Make sure
      the name of the token is `k8s_token`, and the value is the complete
      string created in the previous step.
2. Clone this source repository and `cd` to its root.
3. Activate a conda environment with a current version of `ae5-tools`.
4. Edit the file `anaconda-project.yml` with your preferred editor:
   1. Make sure that all of the packages listed in the `packages:`
      section are available in your internal repository.
   2. Modify the `channels:` section as needed so that it includes the
      channels where those packages reside.
5. Run the following commands as any normal AE5 user:
   1. `ae5 project upload . --name k8s`
   2. `ae5 deployment start k8s --endpoint k8s --private --wait`
   3. `ae5 deployment collaborator add k8s everyone --group`
   4. `ae5 call --endpoint k8s /`
   
   This last command should return the text `Alive and kicking`.
6. If you need to update the deployment, you should remove the existing
   deployment and project first:
   1. `ae5 deployment stop k8s --yes`
   2. `ae5 project delete k8s --yes`
   
   Note that Kubernetes sometimes takes 1-2 minutes to reclaim a
   static endpoint for reuse. So if you immediately re-run the installation
   steps here, you may receive errors of the form
   `Error: endpoint "k8s" is already in use`. If so, just wait a
   minute and try the `deployment start` command again.
      
When installed properly, the deployment will be available
to all authenticated users of the platform. They will be able to
execute the `node info` command and receive a full summary of the
usage on each node. However, the `pod list` and `session`/`deployment`
commands will reveal information only about the sessions, deployments,
and job runs that would ordinarily be visible to them.

Your feedback on the value of this new capability would be greatly appreciated!
Please feel free to file an issue on the [`ae5-tools` issue tracker](https://github.com/Anaconda-Platform/ae5-tools/issues) with your requests.