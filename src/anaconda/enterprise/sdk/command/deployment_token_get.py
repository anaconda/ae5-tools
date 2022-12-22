from typing import Union

from ..ae.session.admin import AEAdminSession
from ..ae.session.user import AEUserSession
from ..command.abstract_command import AbstractCommand
from ..contract.dto.request.deployment_token import DeploymentTokenRequest
from ..contract.dto.response.deployment_token import DeploymentTokenResponse


class DeploymentTokenGetCommand(AbstractCommand):
    def execute(
        self, request: DeploymentTokenRequest, session: Union[AEAdminSession, AEUserSession]
    ) -> DeploymentTokenResponse:
        response = session._post(f"deployments/{request.ident}/token")
        return DeploymentTokenResponse.parse_obj(response)
