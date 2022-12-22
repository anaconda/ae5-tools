from ..ae.record.project import AERecordProject
from ..base_model import BaseModel


class ProjectsGetResponse(BaseModel):
    records: list[AERecordProject]
