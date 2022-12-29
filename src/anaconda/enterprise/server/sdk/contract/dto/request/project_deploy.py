from typing import Optional, Union

from anaconda.enterprise.server.contracts import BaseModel

from ..types.project_deploy_target import ProjectDeployTargetType


class ProjectDeployRequest(BaseModel):
    name: str  # deployment name
    source: str  # revision_source_url
    revision: str  # revision_name
    resource_profile: str
    command: str
    public: bool
    target: Union[ProjectDeployTargetType, str]  # TODO: the full enumeration is not known.
    static_endpoint: Optional[str]  # If defined is used as the static endpoint for serving.
