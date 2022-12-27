from typing import Union

from anaconda.enterprise.server.contracts import AERecordProject

from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class ProjectPatchCommand(AbstractCommand):
    def execute(self, project: AERecordProject, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._patch(f"projects/{project.id}", json=project.json(by_alias=True))
