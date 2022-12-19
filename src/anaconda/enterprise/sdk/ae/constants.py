# Maximum page size in keycloak
from ..common.config.environment import demand_env_var_as_int, get_env_var

KEYCLOAK_PAGE_MAX: int = (
    demand_env_var_as_int(name="KEYCLOAK_PAGE_MAX") if get_env_var(name="KEYCLOAK_PAGE_MAX") else 1000
)

# Maximum number of ids to pass through json body to the k8s endpoint
K8S_JSON_LIST_MAX: int = (
    demand_env_var_as_int(name="K8S_JSON_LIST_MAX") if get_env_var(name="K8S_JSON_LIST_MAX") else 100
)

# Default subdomain for kubectl service
DEFAULT_K8S_ENDPOINT: str = "k8s"

K8S_COLUMNS: tuple = (
    "phase",
    "since",
    "rst",
    "usage/mem",
    "usage/cpu",
    "usage/gpu",
    "changes",
    "modified",
    "node",
)

# Column labels prefixed with a '?' are not included in an initial empty record list.
# For instance, if the --collaborators flag is not set, then projects do not include a
# "collaborators" column. This allows us to provide a consistent header for record outputs
# even when the list is empty.
COLUMNS: dict = {
    "project": [
        "name",
        "owner",
        "?collaborators",
        "editor",
        "resource_profile",
        "id",
        "created",
        "updated",
        "project_create_status",
        "s3_bucket",
        "s3_path",
        "git_server",
        "repository",
        "repo_owned",
        "git_repos",
        "repo_url",
        "url",
    ],
    "revision": [
        "name",
        "latest",
        "owner",
        "commands",
        "created",
        "updated",
        "id",
        "url",
    ],
    "command": ["id", "supports_http_options", "unix", "windows", "env_spec"],
    "collaborator": ["id", "permission", "type", "first_name", "last_name", "email"],
    "session": [
        "name",
        "owner",
        "?usage/mem",
        "?usage/cpu",
        "?usage/gpu",
        "?modified",
        "?node",
        "?rst",
        "resource_profile",
        "id",
        "created",
        "updated",
        "state",
        "?phase",
        "?since",
        "?rst",
        "project_id",
        "session_name",
        "project_branch",
        "iframe_hosts",
        "url",
        "project_url",
    ],
    "resource_profile": ["name", "description", "cpu", "memory", "gpu", "id"],
    "editor": ["name", "id", "is_default", "packages"],
    "sample": [
        "name",
        "id",
        "is_template",
        "is_default",
        "description",
        "download_url",
        "owner",
        "created",
        "updated",
    ],
    "deployment": [
        "endpoint",
        "name",
        "owner",
        "?usage/mem",
        "?usage/cpu",
        "?usage/gpu",
        "?node",
        "?rst",
        "public",
        "?collaborators",
        "command",
        "revision",
        "resource_profile",
        "id",
        "created",
        "updated",
        "state",
        "?phase",
        "?since",
        "?rst",
        "project_id",
        "project_name",
        "project_owner",
    ],
    "job": [
        "name",
        "owner",
        "command",
        "revision",
        "resource_profile",
        "id",
        "created",
        "updated",
        "state",
        "project_id",
        "project_name",
    ],
    "run": [
        "name",
        "owner",
        "command",
        "revision",
        "resource_profile",
        "id",
        "created",
        "updated",
        "state",
        "project_id",
        "project_name",
    ],
    "branch": ["branch", "sha1"],
    "change": ["path", "change_type", "modified", "conflicted", "id"],
    "user": ["username", "firstName", "lastName", "lastLogin", "email", "id"],
    "activity": [
        "type",
        "status",
        "message",
        "done",
        "owner",
        "id",
        "description",
        "created",
        "updated",
    ],
    "endpoint": [
        "id",
        "owner",
        "name",
        "project_name",
        "deployment_id",
        "project_id",
        "project_url",
    ],
    "pod": [
        "name",
        "owner",
        "type",
        "usage/mem",
        "usage/cpu",
        "usage/gpu",
        "node",
        "rst",
        "modified",
        "phase",
        "since",
        "resource_profile",
        "id",
        "project_id",
    ],
}

IDENT_FILTERS: dict = {
    "endpoint": "id={value}",
    "editor": "name={value}|id={value}",
    "node": "name={value}",
    "resource_profile": "name={value}",
    "sample": "name={value}|id={value}",
    "collaborator": "id={value}",
    "user": "username={value}|id={value}",
}

_DTYPES: dict = {
    "created": "datetime",
    "updated": "datetime",
    "since": "datetime",
    "mtime": "datetime",
    "timestamp": "datetime",
    "createdTimestamp": "timestamp/ms",
    "notBefore": "timestamp/s",
    "lastLogin": "timestamp/ms",
    "time": "timestamp/ms",
}
