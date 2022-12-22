from typing import Any, Union

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ...contract.dto.response.secret_get import SecretGetResponse
from ..abstract_command import AbstractCommand


class SecretGetCommand(AbstractCommand):
    def execute(self, session: Union[AEAdminSession, AEUserSession]) -> SecretGetResponse:
        records = session._get("credentials/user")
        if "data" in records:
            return SecretGetResponse(secrets=records["data"])
        return SecretGetResponse(secrets=[])
