from typing import Optional

from ..base_model import BaseModel


class ClusterOptions(BaseModel):
    hostname: Optional[str] = None

    username: Optional[str] = None
    password: Optional[str] = None

    admin_username: Optional[str] = None
    admin_password: Optional[str] = None
    impersonate: bool = False

    ident_filter: tuple = ()
    filter: tuple = ()
    format: Optional[str] = None
    k8s_endpoint: Optional[str] = None
