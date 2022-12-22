from typing import Any, Union

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ...contract.dto.request.secret_put import SecretPutRequest
from ..abstract_command import AbstractCommand


class SecretPutCommand(AbstractCommand):
    def execute(self, request: SecretPutRequest, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._post("credentials/user", json=request.json(by_alias=True))
