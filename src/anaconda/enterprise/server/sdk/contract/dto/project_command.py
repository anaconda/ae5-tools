from typing import Optional

from anaconda.enterprise.server.contracts import BaseModel


class ProjectCommand(BaseModel):
    id: str
    description: str
    env_spec: str
    supports_http_options: bool
    unix: str
    default: Optional[bool]
