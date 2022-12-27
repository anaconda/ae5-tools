from typing import Union

from ...command.abstract_command import AbstractCommand
from ...session.admin import AEAdminSession
from ...session.user import AEUserSession


class ProjectDeleteCommand(AbstractCommand):
    def execute(self, id: str, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._delete(f"projects/{id}")
