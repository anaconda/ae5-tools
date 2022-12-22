from typing import Any, Optional, Union

from .ae.session.admin import AEAdminSession
from .ae.session.factory import AESessionFactory
from .ae.session.user import AEUserSession
from .command.deployment.token_get import DeploymentTokenGetCommand
from .command.secret.delete import SecretDeleteCommand
from .command.secret.get import SecretGetCommand
from .command.secret.put import SecretPutCommand
from .contract.dto.base_model import BaseModel
from .contract.dto.error.ae_error import AEError
from .contract.dto.request.deployment_token import DeploymentTokenRequest
from .contract.dto.request.secret_delete import SecretDeleteRequest
from .contract.dto.request.secret_put import SecretPutRequest
from .contract.dto.response.deployment_token import DeploymentTokenResponse
from .contract.dto.response.secret_get import SecretGetResponse


class AEClient(BaseModel):
    session_factory: AESessionFactory

    # Deployment Commands
    deployment_token_get_command: Optional[DeploymentTokenGetCommand]

    # `Secret` Commands
    secret_put_command: Optional[SecretPutCommand]
    secret_get_command: Optional[SecretGetCommand]
    secret_delete_command: Optional[SecretDeleteCommand]

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.deployment_token_get_command:
            self.deployment_token_get_command = DeploymentTokenGetCommand()
        if not self.secret_put_command:
            self.secret_put_command = SecretPutCommand()
        if not self.secret_get_command:
            self.secret_get_command = SecretGetCommand()
        if not self.secret_delete_command:
            self.secret_delete_command = SecretDeleteCommand()

    # Deployment Commands

    def deployment_token_get(self, ident: str, admin: bool = False) -> str:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        request: DeploymentTokenRequest = DeploymentTokenRequest(ident=ident)
        response: DeploymentTokenResponse = self.deployment_token_get_command.execute(request=request, session=session)
        return response.token

    # `Secret` Commands

    def secret_put(self, key: str, value: str, admin: bool = False) -> None:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        request: SecretPutRequest = SecretPutRequest(key=key, value=value)
        self.secret_put_command.execute(request=request, session=session)

    def secret_get(self, admin: bool = False) -> list[str]:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        response: SecretGetResponse = self.secret_get_command.execute(session=session)
        return response.secrets

    def secret_delete(self, key: str, admin: bool = False) -> None:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        secrets: list[str] = self.secret_get(admin=admin)
        if key not in secrets:
            raise AEError(f"User secret {key} was not found and cannot be deleted.")
        request: SecretDeleteRequest = SecretDeleteRequest(key=key)
        self.secret_delete_command.execute(request=request, session=session)
