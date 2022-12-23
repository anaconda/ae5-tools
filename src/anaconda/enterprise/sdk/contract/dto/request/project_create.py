from typing import Optional

from ..base_model import BaseModel


class ProjectCreateRequest(BaseModel):
    name: Optional[str] = None
    source: str
    tag: Optional[str] = None
    make_unique: bool = True
