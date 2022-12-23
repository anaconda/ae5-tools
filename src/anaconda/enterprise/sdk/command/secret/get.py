from typing import Union

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ...contract.dto.response.secret_names_get import SecretNamesGetResponse
from ..abstract_command import AbstractCommand


class SecretNamesGetCommand(AbstractCommand):
    def execute(self, session: Union[AEAdminSession, AEUserSession]) -> SecretNamesGetResponse:
        records = session._get("credentials/user")
        if "data" in records:
            return SecretNamesGetResponse(secrets=records["data"])
        return SecretNamesGetResponse(secrets=[])
