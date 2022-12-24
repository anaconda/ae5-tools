from typing import Union

from anaconda.enterprise.sdk.session.admin import AEAdminSession
from anaconda.enterprise.sdk.session.user import AEUserSession

from ...command.abstract_command import AbstractCommand


class ProjectDeleteCommand(AbstractCommand):
    def execute(self, id: str, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._delete(f"projects/{id}")
