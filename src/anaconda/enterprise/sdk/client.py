from copy import deepcopy
from typing import Any, Optional, Union

from .ae.session.admin import AEAdminSession
from .ae.session.factory import AESessionFactory
from .ae.session.user import AEUserSession
from .command.deployment.token_get import DeploymentTokenGetCommand
from .command.project.delete import ProjectDeleteCommand
from .command.project.get import ProjectsGetCommand
from .command.project.patch import ProjectPatchCommand
from .command.secret.delete import SecretDeleteCommand
from .command.secret.get import SecretNamesGetCommand
from .command.secret.put import SecretPutCommand
from .contract.dto.ae.record.project import AERecordProject
from .contract.dto.base_model import BaseModel
from .contract.dto.error.ae_error import AEError
from .contract.dto.request.deployment_token import DeploymentTokenRequest
from .contract.dto.request.projects_get import ProjectsGetRequest
from .contract.dto.request.secret_delete import SecretDeleteRequest
from .contract.dto.request.secret_put import SecretPutRequest
from .contract.dto.response.deployment_token import DeploymentTokenResponse
from .contract.dto.response.projects_get import ProjectsGetResponse
from .contract.dto.response.secret_names_get import SecretNamesGetResponse


class AEClient(BaseModel):
    session_factory: AESessionFactory

    # Deployment Commands
    deployment_token_get_command: Optional[DeploymentTokenGetCommand]

    # `Secret` Commands
    secret_put_command: Optional[SecretPutCommand]
    secret_names_get_command: Optional[SecretNamesGetCommand]
    secret_delete_command: Optional[SecretDeleteCommand]

    # Project Commands
    projects_get_command: Optional[ProjectsGetCommand]
    project_patch_command: Optional[ProjectPatchCommand]
    project_delete_command: Optional[ProjectDeleteCommand]

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.deployment_token_get_command:
            self.deployment_token_get_command = DeploymentTokenGetCommand()
        if not self.secret_put_command:
            self.secret_put_command = SecretPutCommand()
        if not self.secret_names_get_command:
            self.secret_names_get_command = SecretNamesGetCommand()
        if not self.secret_delete_command:
            self.secret_delete_command = SecretDeleteCommand()
        if not self.projects_get_command:
            self.projects_get_command = ProjectsGetCommand()
        if not self.project_patch_command:
            self.project_patch_command = ProjectPatchCommand()
        if not self.project_delete_command:
            self.project_delete_command = ProjectDeleteCommand()

    # Deployment Commands

    def deployment_token_get(self, id: str, admin: bool = False) -> str:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        request: DeploymentTokenRequest = DeploymentTokenRequest(id=id)
        response: DeploymentTokenResponse = self.deployment_token_get_command.execute(request=request, session=session)
        return response.token

    # `Secret` Commands

    def secret_put(self, key: str, value: str, admin: bool = False) -> None:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        request: SecretPutRequest = SecretPutRequest(key=key, value=value)
        self.secret_put_command.execute(request=request, session=session)

    def secret_names_get(self, admin: bool = False) -> list[str]:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        response: SecretNamesGetResponse = self.secret_names_get_command.execute(session=session)
        return response.secrets

    def secret_delete(self, key: str, admin: bool = False) -> None:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        secrets: list[str] = self.secret_names_get(admin=admin)
        if key not in secrets:
            raise AEError(f"User secret {key} was not found and cannot be deleted.")
        request: SecretDeleteRequest = SecretDeleteRequest(key=key)
        self.secret_delete_command.execute(request=request, session=session)

    # Project Commands
    def projects_get(
        self, filter: Optional[str] = None, collaborators: bool = False, admin: bool = False
    ) -> list[AERecordProject]:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        request: ProjectsGetRequest = ProjectsGetRequest(filter=filter, collaborators=collaborators)
        response: ProjectsGetResponse = self.projects_get_command.execute(request=request, session=session)
        return response.records

    def project_get(
        self, id: str, filter: Optional[str] = None, collaborators: bool = False, admin: bool = False
    ) -> Optional[AERecordProject]:
        records: list[AERecordProject] = self.projects_get(filter=filter, collaborators=collaborators, admin=admin)
        for record in records:
            if record.id == id:
                return record

    def project_patch(
        self, project: dict, filter: Optional[str] = None, collaborators: bool = False, admin: bool = False
    ) -> Optional[AERecordProject]:
        # Note:
        # This is not transactional (in that if the record changes after we read it but before we commit changes we will lose data).

        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)

        if "id" not in project:
            raise AEError(f"No project id specified to patch")

        previous_record: Optional[AERecordProject] = self.project_get(
            id=project["id"], filter=filter, collaborators=collaborators, admin=admin
        )
        if previous_record is None:
            raise AEError(f"No existing project {project['id']} found to patch")

        project_proto: dict = {**previous_record.dict(by_alias=True), **project}
        new_project: AERecordProject = AERecordProject.parse_obj(project_proto)
        self.project_patch_command.execute(project=new_project, session=session)

        return self.project_get(id=new_project.id, filter=filter, collaborators=collaborators, admin=admin)

    def project_delete(self, id: str, admin: bool = False) -> None:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        self.project_delete_command.execute(id=id, session=session)
