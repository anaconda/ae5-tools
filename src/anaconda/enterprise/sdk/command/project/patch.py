from typing import Any, Union

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ...contract.dto.ae.record.project import AERecordProject
from ..abstract_command import AbstractCommand


class ProjectPatchCommand(AbstractCommand):
    def execute(self, project: AERecordProject, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._patch(f"projects/{project.id}", json=project.json(by_alias=True))
