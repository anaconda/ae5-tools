from typing import Union

from anaconda.enterprise.sdk.session.admin import AEAdminSession
from anaconda.enterprise.sdk.session.user import AEUserSession
from anaconda.enterprise.server.contracts import SecretPutRequest

from ..abstract_command import AbstractCommand


class SecretPutCommand(AbstractCommand):
    def execute(self, request: SecretPutRequest, session: Union[AEAdminSession, AEUserSession]) -> None:
        session._post("credentials/user", json=request.dict(by_alias=True))
