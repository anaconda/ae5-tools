from pathlib import Path
from typing import Any, Optional, Union

from anaconda.enterprise.server.contracts import (
    AEError,
    AERecordProject,
    BaseModel,
    DeploymentTokenRequest,
    DeploymentTokenResponse,
    ProjectsGetRequest,
    ProjectsGetResponse,
    SecretDeleteRequest,
    SecretNamesGetResponse,
    SecretPutRequest,
)

from .command.deployment.token_get import DeploymentTokenGetCommand
from .command.project.delete import ProjectDeleteCommand
from .command.project.get import ProjectsGetCommand
from .command.project.patch import ProjectPatchCommand
from .command.project.revisions_get import ProjectRevisionsGetCommand
from .command.project.upload import ProjectUploadCommand
from .command.secret.delete import SecretDeleteCommand
from .command.secret.get import SecretNamesGetCommand
from .command.secret.put import SecretPutCommand
from .contract.dto.project_revision import ProjectRevision
from .contract.dto.request.project_upload import ProjectUploadRequest
from .contract.dto.response.project_revisions_get import ProjectRevisionsGetResponse
from .contract.dto.response.project_upload import ProjectUploadResponse
from .session.admin import AEAdminSession
from .session.factory import AESessionFactory
from .session.user import AEUserSession


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
    project_upload_command: Optional[ProjectUploadCommand]
    project_revisions_get_command: Optional[ProjectRevisionsGetCommand]

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
        if not self.project_upload_command:
            self.project_upload_command = ProjectUploadCommand()
        if not self.project_revisions_get_command:
            self.project_revisions_get_command = ProjectRevisionsGetCommand()

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

    def project_upload(
        self, project_archive_path: str, tag: str, name: str, admin: bool = False
    ) -> ProjectUploadResponse:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        request: ProjectUploadRequest = ProjectUploadRequest(
            project_archive_path=Path(project_archive_path), tag=tag, name=name
        )
        return self.project_upload_command.execute(request=request, session=session)

    def project_revisions_get(self, project_id: str, admin: bool = False) -> list[ProjectRevision]:
        session: Union[AEAdminSession, AEUserSession] = self.session_factory.get(admin=admin)
        revisions_response: ProjectRevisionsGetResponse = self.project_revisions_get_command.execute(
            project_id=project_id, session=session
        )
        return revisions_response.revisions
