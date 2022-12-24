from typing import Union

from anaconda.enterprise.sdk.session.admin import AEAdminSession
from anaconda.enterprise.sdk.session.user import AEUserSession
from anaconda.enterprise.server.contracts import SecretDeleteRequest

from ..abstract_command import AbstractCommand


class SecretDeleteCommand(AbstractCommand):
    def execute(self, request: SecretDeleteRequest, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._delete(f"credentials/user/{request.key}")
