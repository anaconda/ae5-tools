import datetime

from anaconda.enterprise.server.contracts import BaseModel

from ..action_summary import AEActionSummary


class ProjectDeployResponse(BaseModel):
    project_owner: str
    owner: str
    id: str
    source: str
    public: bool
    project_name: str
    goal_state: str
    action: AEActionSummary
    revision: str
    updated: datetime.datetime
    variables: dict  # TODO: HOW?
    url: str
    resource_profile: str
    project_url: str
    replicas: int
    command: str
    created: datetime.datetime
    name: str
    git_repos: dict  # TODO: How is this defined?
    state: str  # TODO: Is this an enumeration?
    status_text: str
