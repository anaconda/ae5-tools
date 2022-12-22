from typing import Union

from anaconda.enterprise.sdk.ae.session.admin import AEAdminSession
from anaconda.enterprise.sdk.ae.session.user import AEUserSession
from anaconda.enterprise.sdk.command.abstract_command import AbstractCommand
from anaconda.enterprise.sdk.contract.dto.request.deployment_token import DeploymentTokenRequest
from anaconda.enterprise.sdk.contract.dto.response.deployment_token import DeploymentTokenResponse


class DeploymentTokenGetCommand(AbstractCommand):
    def execute(
        self, request: DeploymentTokenRequest, session: Union[AEAdminSession, AEUserSession]
    ) -> DeploymentTokenResponse:
        response = session._post(f"deployments/{request.ident}/token")
        return DeploymentTokenResponse.parse_obj(response)
