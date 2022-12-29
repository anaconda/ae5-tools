from typing import Union

from ...contract.dto.response.project_revisions_get import ProjectRevisionsGetResponse
from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class ProjectRevisionsGetCommand(AbstractCommand):
    def execute(self, project_id: str, session: Union[AEAdminSession, AEUserSession]) -> ProjectRevisionsGetResponse:
        response = session._api(method="get", endpoint=f"projects/{project_id}/revisions")
        return ProjectRevisionsGetResponse(revisions=response)
