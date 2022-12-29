from anaconda.enterprise.server.contracts import BaseModel

from ..project_revision import ProjectRevision


class ProjectRevisionsGetResponse(BaseModel):
    revisions: list[ProjectRevision]
