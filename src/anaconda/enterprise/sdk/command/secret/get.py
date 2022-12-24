from typing import Union

from anaconda.enterprise.sdk.session.admin import AEAdminSession
from anaconda.enterprise.sdk.session.user import AEUserSession
from anaconda.enterprise.server.contracts import SecretNamesGetResponse

from ..abstract_command import AbstractCommand


class SecretNamesGetCommand(AbstractCommand):
    def execute(self, session: Union[AEAdminSession, AEUserSession]) -> SecretNamesGetResponse:
        records = session._get("credentials/user")
        if "data" in records:
            return SecretNamesGetResponse(secrets=records["data"])
        return SecretNamesGetResponse(secrets=[])
