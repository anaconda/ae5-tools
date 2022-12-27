from typing import Union

from anaconda.enterprise.server.contracts import SecretDeleteRequest

from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class SecretDeleteCommand(AbstractCommand):
    def execute(self, request: SecretDeleteRequest, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._delete(f"credentials/user/{request.key}")
