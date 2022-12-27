from typing import Union

from anaconda.enterprise.server.contracts import AERecordProject, ProjectsGetRequest, ProjectsGetResponse

from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class ProjectsGetCommand(AbstractCommand):
    def execute(
        self,
        request: ProjectsGetRequest,
        session: Union[AEAdminSession, AEUserSession],
    ) -> ProjectsGetResponse:
        records: list[AERecordProject] = session.get_records(type="projects")
        return ProjectsGetResponse(records=records)
