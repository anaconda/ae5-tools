from typing import Any, Optional, Union

from .ae.session.admin import AEAdminSession
from .ae.session.factory import AESessionFactory
from .ae.session.user import AEUserSession
from .command.deployment_token_get import DeploymentTokenGetCommand
from .contract.dto.base_model import BaseModel
from .contract.dto.request.deployment_token import DeploymentTokenRequest
from .contract.dto.response.deployment_token import DeploymentTokenResponse


class AEClient(BaseModel):
    session_factory: AESessionFactory
    deployment_token_get_command: Optional[DeploymentTokenGetCommand]

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.deployment_token_get_command:
            self.deployment_token_get_command = DeploymentTokenGetCommand()

    def deployment_token_get(self, ident: str, admin: bool = False) -> str:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.login(admin=admin)
        request: DeploymentTokenRequest = DeploymentTokenRequest.parse_obj({"ident": ident})
        response: DeploymentTokenResponse = self.deployment_token_get_command.execute(request=request, session=session)
        return response.token
