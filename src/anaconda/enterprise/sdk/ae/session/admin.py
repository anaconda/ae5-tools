from typing import Any, Optional

from .abstract import AbstractAESession


class AEAdminSession(AbstractAESession):
    login_base: Optional[str] = None
    sdata: Optional[dict] = None

    def __init__(self, prefix: str = "auth/admin/realms/AnacondaPlatform", **data: Any):
        super().__init__(**data, prefix=prefix)

        self.sdata = None
        self.login_base = f"https://{self.hostname}/auth/realms/master/protocol/openid-connect"

    def _connected(self):
        return isinstance(self.sdata, dict) and "access_token" in self.sdata

    def _set_header(self):
        self.session.headers["Authorization"] = f'Bearer {self.sdata["access_token"]}'

    def _connect(self, password):
        resp = self.session.post(
            self.login_base + "/token",
            data={"username": self.username, "password": password, "grant_type": "password", "client_id": "admin-cli"},
        )
        self.sdata = {} if resp.status_code == 401 else resp.json()

    def _disconnect(self):
        if self.sdata:
            self.session.post(
                self.login_base + "/logout",
                data={"refresh_token": self.sdata["refresh_token"], "client_id": "admin-cli"},
            )
            self.sdata.clear()
