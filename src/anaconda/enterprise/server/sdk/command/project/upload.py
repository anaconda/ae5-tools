from typing import Union

from anaconda.enterprise.server.contracts import ProjectUploadRequest, ProjectUploadResponse

from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class ProjectUploadCommand(AbstractCommand):
    def execute(
        self, request: ProjectUploadRequest, session: Union[AEAdminSession, AEUserSession]
    ) -> ProjectUploadResponse:
        with open(file=request.project_archive_path.resolve().as_posix(), mode="rb") as project_file:
            records = session._api(
                method="post",
                endpoint="projects/upload",
                files={b"project_file": (request.project_archive_path.name, project_file)},
                data={"name": request.name, "tag": request.tag},
            )
            return ProjectUploadResponse.parse_obj(records)
