from typing import Optional

from ..base_model import BaseModel


class ProjectsGetRequest(BaseModel):
    filter: Optional[str] = None
    collaborators: bool = False
