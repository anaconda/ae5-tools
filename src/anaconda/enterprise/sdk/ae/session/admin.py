import sys
from http.cookiejar import LWPCookieJar
from typing import Any, Optional

from ...cluster.identifier import Identifier
from ...common.config.environment import demand_env_var_as_int
from ..constants import IDENT_FILTERS
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

    def _get_paginated(self, path, **kwargs):
        records = []
        limit = kwargs.pop("limit", sys.maxsize)
        kwargs.setdefault("first", 0)
        while True:
            kwargs["max"] = min(demand_env_var_as_int(name="KEYCLOAK_PAGE_MAX"), limit)
            t_records = self._get(path, params=kwargs)
            records.extend(t_records)
            n_records = len(t_records)
            if n_records < kwargs["max"] or n_records == limit:
                return records
            kwargs["first"] += n_records
            limit -= n_records

    def user_events(self, format=None, **kwargs):
        first = kwargs.pop("first", 0)
        limit = kwargs.pop("limit", sys.maxsize)
        records = self._get_paginated("events", limit=limit, first=first, **kwargs)
        return self._format_response(records, format=format, columns=[])

    def _post_user(self, users, include_login=False):
        users = {u["id"]: u for u in users}
        if include_login:
            events = self._get_paginated("events", client="anaconda-platform", type="LOGIN")
            for e in events:
                if "response_mode" not in e["details"]:
                    urec = users.get(e["userId"])
                    if urec and "lastLogin" not in urec:
                        urec["lastLogin"] = e["time"]
        users = list(users.values())
        for urec in users:
            urec.setdefault("lastLogin", 0)
        return users

    def user_list(self, filter=None, format=None, include_login=True):
        users = self._get_paginated("users")
        users = self._fix_records("user", users, filter, include_login=include_login)
        return self._format_response(users, format=format)

    def user_info(self, ident, format=None, include_login=True):
        response = self._ident_record("user", ident, quiet=False, include_login=include_login)
        return self._format_response(response, format)

    def impersonate(self, user_or_id):
        record = self.user_info(user_or_id, include_login=False)
        old_headers = self.session.headers.copy()
        try:
            self._post(f'users/{record["id"]}/impersonation')
            params = {
                "client_id": "anaconda-platform",
                "scope": "openid",
                "response_type": "code",
                "redirect_uri": f"https://{self.hostname}/login",
            }
            self._get("/auth/realms/AnacondaPlatform/protocol/openid-connect/auth", params=params)
            cookies, self.session.cookies = self.session.cookies, LWPCookieJar()
            return cookies
        finally:
            self.session.cookies.clear()
            self.session.headers = old_headers

    def _ident_record(self, record_type, ident, quiet=False, **kwargs):
        if isinstance(ident, dict) and ident.get("_record_type", "") == record_type:
            return ident
        itype = record_type + "s"
        if isinstance(ident, Identifier):
            filter = ident.project_filter(itype=itype, ignore_revision=True)
        elif isinstance(ident, tuple):
            ident, filter = ",".join(ident), ident
        elif record_type in IDENT_FILTERS:
            ident = filter = IDENT_FILTERS[record_type].format(value=ident)
        else:
            ident = Identifier.from_string(ident, itype)
            filter = ident.project_filter(itype=itype, ignore_revision=True)
        matches = getattr(self, f"{record_type}_list")(filter=filter, **kwargs)
        return self._should_be_one(matches, filter, quiet)
