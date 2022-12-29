from typing import Union

from ...contract.dto.request.project_deploy import ProjectDeployRequest
from ...contract.dto.response.project_deploy import ProjectDeployResponse
from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class ProjectDeployCommand(AbstractCommand):
    def execute(
        self, project_id: str, request: ProjectDeployRequest, session: Union[AEAdminSession, AEUserSession]
    ) -> ProjectDeployResponse:
        data: dict = request.dict(by_alias=False, exclude_none=True)
        response = session._api(method="post", endpoint=f"projects/{project_id}/deployments", json=data)
        return ProjectDeployResponse.parse_obj(response)
