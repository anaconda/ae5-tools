from typing import Optional

from anaconda.enterprise.server.contracts import BaseModel


class ClientOptions(BaseModel):
    hostname: Optional[str] = None

    username: Optional[str] = None
    password: Optional[str] = None

    admin_username: Optional[str] = None
    admin_password: Optional[str] = None
    impersonate: bool = False

    ident_filter: tuple = ()
    filter: tuple = ()
    format: Optional[str] = None
