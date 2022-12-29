import datetime

from anaconda.enterprise.server.contracts import BaseModel

from ..action_summary import AEActionSummary


class ProjectUploadResponse(BaseModel):
    updated: datetime.datetime
    id: str
    owner: str
    name: str
    action: AEActionSummary
