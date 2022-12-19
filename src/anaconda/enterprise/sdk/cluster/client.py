from typing import Any, Optional, Union

from ..ae.session.admin import AEAdminSession
from ..ae.session.user import AEUserSession
from ..contracts.ae_user_session_command import AEUserSessionCommand
from ..contracts.dto.base_model import BaseModel
from ..contracts.dto.cluster.options import ClusterOptions
from ..contracts.dto.error.ae_config_error import AEConfigError
from .config import ConfigManager
from .identifier import Identifier


class ClusterClient(BaseModel):
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

    def _login(self, admin=False) -> Union[AEAdminSession, AEUserSession]:
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
                password = self._login(admin=True)
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

    def call(self, method: AEUserSessionCommand, *args, **kwargs) -> Any:

        admin: bool = kwargs.pop("admin", False)
        cluster_session: Union[AEAdminSession, AEUserSession] = self._login(admin=admin)

        # Provide a standardized method for supplying the filter argument
        # to the *_list api commands, and the ident argument for *_info
        # api commands.

        if self.options.ident_filter:
            record_type, filter, required, revision = self.options.ident_filter
            if required:
                ident = ",".join(filter)
                if method.endswith("_info"):
                    if revision is not None:
                        filter = filter + (f"revision={revision}",)
                    args = (filter,) + args
                else:
                    # For tasks that require a unique record, we call
                    # ident_record to fully resolve the identifier and
                    # ensure that it is unique.
                    result: Optional[dict] = cluster_session.ident_record(record_type, filter)

                    if Identifier.has_prefix(record_type + "s"):
                        ident = Identifier.from_record(result)
                    if revision is not None:
                        result["_revision"] = revision
                    args = (result,) + args
            else:
                # In non-required mode, we can feed the entire filter
                # into the command, the combination of the identifier
                # filter and the command-line filter arguments.
                kwargs["filter"] = filter + self.options.filter
                self.options.filter = ()

        # Do not perform this processing on converted calls [legacy processing]
        if method not in [AEUserSessionCommand.DEPLOYMENT_TOKEN]:
            call_format: Optional[str] = self.options.format
            if call_format is None:
                # This is a special format that passes tabular json data
                # without error, but converts json data to a table
                call_format = "tableif"
            elif call_format in ("json", "csv"):
                call_format = "table"
            kwargs.setdefault("format", call_format)

        # Retrieve the proper cluster session object and make the call
        result = getattr(cluster_session, method)(*args, **kwargs)

        # Do not perform this processing on converted calls [legacy processing]
        if method in [AEUserSessionCommand.DEPLOYMENT_TOKEN]:
            return result

        #  Legacy output
        # Finish out the standardized CLI output
        print(result)
