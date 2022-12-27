from typing import Optional, Union

from anaconda.enterprise.server.contracts import AEConfigError, BaseModel

from ..contract.dto.options import ClientOptions
from .admin import AEAdminSession
from .user import AEUserSession


class AESessionFactory(BaseModel):
    options: ClientOptions
    SESSIONS: dict = {}

    def _get_account(self, admin=False) -> tuple[str, str]:
        hostname: str = self.options.hostname
        if admin:
            username = self.options.admin_username
        else:
            username = self.options.username

        if hostname and username:
            return hostname, username

        raise AEConfigError("Unable to resolve AE hostname and/or user")

    def get(self, admin=False) -> Union[AEAdminSession, AEUserSession]:
        hostname, username = self._get_account(admin=admin)
        return self._connect(hostname, username, admin)

    def _connect(
        self, hostname: str, username: str, admin: bool, silent: bool = True
    ) -> Union[AEAdminSession, AEUserSession]:
        key: tuple[str, str, bool] = (hostname, username, admin)
        conn: Optional[tuple[str, str, bool]] = self.SESSIONS.get(key)

        if conn:
            return self.SESSIONS.get(key)

        if not silent:
            atype: str = "admin" if admin else "user"
            print(f"Connecting to {atype} account {username}@{hostname}.")

        if admin:
            conn: AEAdminSession = AEAdminSession(
                hostname=hostname,
                username=username,
                password=self.options.admin_password,
            )
        else:
            if self.options.impersonate:
                password = self.get(admin=True)
            else:
                password = self.options.password

            conn: AEUserSession = AEUserSession(
                hostname=hostname,
                username=username,
                password=password,
            )
        self.SESSIONS[key] = conn
        return conn
