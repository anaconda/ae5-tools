from typing import Union

from anaconda.enterprise.sdk.session.admin import AEAdminSession
from anaconda.enterprise.sdk.session.user import AEUserSession
from anaconda.enterprise.server.contracts import DeploymentTokenRequest, DeploymentTokenResponse

from ...command.abstract_command import AbstractCommand


class DeploymentTokenGetCommand(AbstractCommand):
    def execute(
        self, request: DeploymentTokenRequest, session: Union[AEAdminSession, AEUserSession]
    ) -> DeploymentTokenResponse:
        response = session._post(f"deployments/{request.id}/token")
        return DeploymentTokenResponse.parse_obj(response)
