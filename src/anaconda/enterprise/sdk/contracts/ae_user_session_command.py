from enum import Enum


class AEUserSessionCommand(str, Enum):

    # Deployment
    DEPLOYMENT_INFO = "deployment_info"
    DEPLOYMENT_LIST = "deployment_list"
    DEPLOYMENT_LOGS = "deployment_logs"
    DEPLOYMENT_OPEN = "deployment_open"
    DEPLOYMENT_PATCH = "deployment_patch"
    DEPLOYMENT_RESTART = "deployment_restart"
    DEPLOYMENT_START = "deployment_start"
    DEPLOYMENT_STOP = "deployment_stop"
    DEPLOYMENT_TOKEN = "deployment_token"  # Converted to new API

    # Deployment Collaborator
    DEPLOYMENT_COLLABORATOR_ADD = "deployment_collaborator_add"
    DEPLOYMENT_COLLABORATOR_INFO = "deployment_collaborator_info"
    DEPLOYMENT_COLLABORATOR_LIST = "deployment_collaborator_list"
    DEPLOYMENT_COLLABORATOR_LIST_SET = "deployment_collaborator_list_set"
    DEPLOYMENT_COLLABORATOR_REMOVE = "deployment_collaborator_remove"

    # Editor
    EDITOR_INFO = "editor_info"
    EDITOR_LIST = "editor_list"

    # Endpoint
    ENDPOINT_INFO = "endpoint_info"
    ENDPOINT_LIST = "endpoint_list"

    # Job
    JOB_CREATE = "job_create"
    JOB_DELETE = "job_delete"
    JOB_INFO = "job_info"
    JOB_LIST = "job_list"
    JOB_PATCH = "job_patch"
    JOB_PAUSE = "job_pause"
    JOB_RUNS = "job_runs"
    JOB_UNPAUSE = "job_unpause"

    # Project
    PROJECT_ACTIVITY = "project_activity"
    PROJECT_CREATE = "project_create"
    PROJECT_DELETE = "project_delete"
    PROJECT_DEPLOYMENTS = "project_deployments"
    PROJECT_DOWNLOAD = "project_download"
    PROJECT_INFO = "project_info"
    PROJECT_IMAGE = "project_image"
    PROJECT_JOBS = "project_jobs"
    PROJECT_LIST = "project_list"
    PROJECT_PATCH = "project_patch"
    PROJECT_RUNS = "project_runs"
    PROJECT_SESSIONS = "project_sessions"
    PROJECT_UPLOAD = "project_upload"

    # Project Collaborator
    PROJECT_COLLABORATOR_ADD = "project_collaborator_add"
    PROJECT_COLLABORATOR_INFO = "project_collaborator_info"
    PROJECT_COLLABORATOR_LIST = "project_collaborator_list"
    PROJECT_COLLABORATOR_LIST_SET = "project_collaborator_list_set"
    PROJECT_COLLABORATOR_REMOVE = "project_collaborator_remove"

    # Resource Profile
    RESOURCE_PROFILE_INFO = "resource_profile_info"
    RESOURCE_PROFILE_LIST = "resource_profile_list"

    # Revision
    REVISION_COMMANDS = "revision_commands"
    REVISION_INFO = "revision_info"
    REVISION_LIST = "revision_list"

    # Run
    RUN_DELETE = "run_delete"
    RUN_LIST = "run_list"
    RUN_LOG = "run_log"
    RUN_INFO = "run_info"
    RUN_STOP = "run_stop"

    # Sample
    SAMPLE_CLONE = "sample_clone"
    SAMPLE_LIST = "sample_list"
    SAMPLE_INFO = "sample_info"

    # Secret
    SECRET_ADD = "secret_add"
    SECRET_DELETE = "secret_delete"
    SECRET_LIST = "secret_list"

    # Session
    SESSION_BRANCHES = "session_branches"
    SESSION_CHANGES = "session_changes"
    SESSION_INFO = "session_info"
    SESSION_LIST = "session_list"
    SESSION_OPEN = "session_open"
    SESSION_RESTART = "session_restart"
    SESSION_START = "session_start"
    SESSION_STOP = "session_stop"

    # User [Admin]
    USER_EVENTS = "user_events"
    USER_LIST = "user_list"
    USER_INFO = "user_info"

    # K8s [Admin]
    POD_INFO = "pod_info"
    POD_LIST = "pod_list"
    NODE_INFO = "node_info"
    NODE_LIST = "node_list"
