from typing import Union

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ...contract.dto.request.projects_get import ProjectsGetRequest
from ...contract.dto.response.projects_get import ProjectsGetResponse
from ..abstract_command import AbstractCommand


class ProjectsGetCommand(AbstractCommand):
    def execute(
        self,
        request: ProjectsGetRequest,
        session: Union[AEAdminSession, AEUserSession],
    ) -> ProjectsGetResponse:
        records_raw: list[dict] = session._get_records(
            "projects", filter=request.filter, collaborators=request.collaborators
        )
        # records: list[AERecordProject] = []
        # for record in records_raw:
        #     records.append(AERecordProject.parse_obj(record))
        return ProjectsGetResponse.parse_obj({"records": records_raw})
