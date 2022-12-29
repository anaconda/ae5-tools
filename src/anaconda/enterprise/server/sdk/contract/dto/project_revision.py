import datetime

from anaconda.enterprise.server.contracts import BaseModel

from .project_command import ProjectCommand


class ProjectRevision(BaseModel):
    id: str
    name: str
    url: str
    owner: str
    created: datetime.datetime
    updated: datetime.datetime
    commands: list[ProjectCommand]
