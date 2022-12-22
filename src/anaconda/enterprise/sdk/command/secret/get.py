from typing import Any, Union

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ..abstract_command import AbstractCommand


class SecretGetCommand(AbstractCommand):
    def execute(self, session: Union[AEAdminSession, AEUserSession]) -> list[str]:
        # TODO: I have not seen what the response for this is yet,
        # need to create DTO for it once its been confirmed.
        records = session._get("credentials/user")
        if "data" in records:
            return records["data"]
        return []
