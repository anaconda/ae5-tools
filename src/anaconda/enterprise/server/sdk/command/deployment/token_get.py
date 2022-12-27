from typing import Union

from anaconda.enterprise.server.contracts import DeploymentTokenRequest, DeploymentTokenResponse

from ...command.abstract_command import AbstractCommand
from ...session.admin import AEAdminSession
from ...session.user import AEUserSession


class DeploymentTokenGetCommand(AbstractCommand):
    def execute(
        self, request: DeploymentTokenRequest, session: Union[AEAdminSession, AEUserSession]
    ) -> DeploymentTokenResponse:
        response = session._post(f"deployments/{request.id}/token")
        return DeploymentTokenResponse.parse_obj(response)
