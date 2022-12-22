from typing import Optional, Union

from ...contract.dto.base_model import BaseModel
from ...contract.dto.cluster.options import ClusterOptions
from ...contract.dto.error.ae_config_error import AEConfigError
from ...service.config import ConfigManager
from ..session.admin import AEAdminSession
from ..session.user import AEUserSession


class AESessionFactory(BaseModel):
    options: ClusterOptions
    config: ConfigManager
    SESSIONS: dict = {}

    def _get_account(self, admin=False) -> tuple[str, str]:
        hostname: str = self.options.hostname
        if admin:
            username = self.options.admin_username
        else:
            username = self.options.username

        if hostname and username:
            return hostname, username

        matches: list[tuple[str, str]] = self.config.resolve(hostname, username, admin)
        if len(matches) >= 1:
            hostname, username = matches[0]
        else:
            raise AEConfigError("Unable to resolve AE hostname and/or user")

        self.options.hostname = hostname
        if admin:
            self.options.admin_username = username
        else:
            self.options.username = username

        return hostname, username

    def login(self, admin=False) -> Union[AEAdminSession, AEUserSession]:
        hostname, username = self._get_account(admin=admin)
        return self._connect(hostname, username, admin)

    def _connect(self, hostname: str, username: str, admin: bool) -> Union[AEAdminSession, AEUserSession]:
        key: tuple[str, str, bool] = (hostname, username, admin)
        conn: Optional[tuple[str, str, bool]] = self.SESSIONS.get(key)

        if conn:
            return self.SESSIONS.get(key)

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
                password = self.login(admin=True)
            else:
                password = self.options.password

            conn: AEUserSession = AEUserSession(
                hostname=hostname,
                username=username,
                password=password,
                k8s_endpoint=self.options.k8s_endpoint,
            )
        self.SESSIONS[key] = conn
        return conn
