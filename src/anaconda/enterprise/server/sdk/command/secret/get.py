from typing import Union

from anaconda.enterprise.server.contracts import SecretNamesGetResponse

from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class SecretNamesGetCommand(AbstractCommand):
    def execute(self, session: Union[AEAdminSession, AEUserSession]) -> SecretNamesGetResponse:
        records = session._get("credentials/user")
        if "data" in records:
            return SecretNamesGetResponse(secrets=records["data"])
        return SecretNamesGetResponse(secrets=[])
