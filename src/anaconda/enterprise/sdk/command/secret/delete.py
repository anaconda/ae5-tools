from typing import Union

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ...contract.dto.request.secret_delete import SecretDeleteRequest
from ..abstract_command import AbstractCommand


class SecretDeleteCommand(AbstractCommand):
    def execute(self, request: SecretDeleteRequest, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._delete(f"credentials/user/{request.key}")
