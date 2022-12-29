from pathlib import Path

from anaconda.enterprise.server.contracts import BaseModel


class ProjectUploadRequest(BaseModel):
    project_archive_path: Path
    tag: str
    name: str
