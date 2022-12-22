import datetime
from typing import Union

from ....ae_record_project_create_status_type import AERecordProjectCreateStatusType
from ....ae_record_project_editor_type import AERecordProjectEditorType
from ....ae_record_project_resource_profile_type import AERecordProjectResourceProfileType
from .abstract import AbstractAERecord


class AERecordProject(AbstractAERecord):
    updated: datetime.datetime
    repo_url: str
    url: str
    repository: str
    id: str
    editor: Union[AERecordProjectEditorType, str]
    resource_profile: Union[AERecordProjectResourceProfileType, str]
    owner: str
    git_repos: dict  # Further details are needed in order to full define this DTO
    project_create_status: Union[AERecordProjectCreateStatusType, str]
    created: datetime.datetime
    repo_owned: bool
    name: str
    git_server: str
