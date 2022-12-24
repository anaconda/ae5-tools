import re
import sys
import time
from abc import abstractmethod
from http.cookiejar import LWPCookieJar
from typing import Any, AnyStr, Match, Optional, Union

import requests
from requests import Response, Session

from anaconda.enterprise.server.contracts import AERecordProject, AEUnexpectedResponseError, BaseModel


class AbstractAESession(BaseModel):
    """Base class for AE5 API interactions.

    Base class constructor.
    Args:
        hostname: The FQDN of the AE5 cluster
        username: The username associated with the connection.
        password (str, AEAdminSession, or None): nominally, this is
            the password used to log in, if it is necessary. If password=None, and
            the session has expired, it will prompt the user for a password. If
            password is an AEAdminSession, it will be used to impersonate the user.
        prefix (str): The URL prefix to prepend to all API calls.
        persist: if True, an attempt will be made to load the session from disk;
            and if a new login is required, it will save the session to disk. If
            false, session information will neither be loaded nor saved.
    """

    hostname: str
    username: str
    password: Optional[str] = None
    prefix: str = "api/v2"
    session: Optional[Session] = None

    @abstractmethod
    def _connected(self) -> bool:
        """ """

    @abstractmethod
    def _set_header(self):
        """ """

    @abstractmethod
    def _disconnect(self):
        """ """

    # def __init__(self, hostname, username, password, prefix, persist):
    def __init__(self, **data: Any):
        """Base class constructor.
        Args:
            hostname: The FQDN of the AE5 login
            username: The username associated with the connection.
            password (str, AEAdminSession, or None): nominally, this is
                the password used to log in, if it is necessary. If password=None, and
                the session has expired, it will prompt the user for a password. If
                password is an AEAdminSession, it will be used to impersonate the user.
            prefix (str): The URL prefix to prepend to all API calls.
            persist: if True, an attempt will be made to load the session from disk;
                and if a new login is required, it will save the session to disk. If
                false, session information will neither be loaded nor saved.
        """

        super().__init__(**data)

        self.prefix = self.prefix.lstrip("/")

        self.session: Session = requests.Session()
        self.session.verify = False
        self.session.cookies = LWPCookieJar()

        if self._connected():
            self._set_header()

    def __del__(self):
        # Try to be a good citizen and shut down the active session.
        # But fail silently if it does not work. In particular, if this
        # destructor is called too late in the shutdown process, the call
        # to requests will fail with an ImportError.
        if sys.meta_path is not None and self._connected():
            try:
                self.disconnect()
            except Exception:
                pass

    def authorize(self):
        self._connect(password=self.password)
        if self._connected():
            self._set_header()

    def disconnect(self):
        self._disconnect()
        self.session.headers.clear()
        self.session.cookies.clear()

    def _api(self, method, endpoint, **kwargs):
        format = kwargs.pop("format", None)
        subdomain = kwargs.pop("subdomain", None)
        isabs, endpoint = endpoint.startswith("/"), endpoint.lstrip("/")
        if subdomain:
            subdomain += "."
            isabs = True
        else:
            subdomain = ""
        if not isabs:
            endpoint = f"{self.prefix}/{endpoint}"
        url = f"https://{subdomain}{self.hostname}/{endpoint}"
        do_save = False
        allow_retry = True
        if not self._connected():
            self.authorize()
            if self.password is not None:
                allow_retry = False
        retries = redirects = 0
        while True:
            try:
                response = getattr(self.session, method)(url, allow_redirects=False, **kwargs)
                retries = 0
            except requests.exceptions.ConnectionError:
                if retries == 3:
                    raise AEUnexpectedResponseError("Unable to connect", method, url, **kwargs)
                retries += 1
                time.sleep(2)
                continue
            except requests.exceptions.Timeout:
                raise AEUnexpectedResponseError("Connection timeout", method, url, **kwargs)
            if 300 <= response.status_code < 400:
                # Redirection here happens for two reasons, described below. We
                # handle them ourselves to provide better behavior than requests.
                url2 = response.headers["location"].rstrip()
                if url2.startswith("/"):
                    url2 = f"https://{subdomain}{self.hostname}{url2}"
                if url2 == url:
                    # Self-redirects happen sometimes when the deployment is not
                    # fully ready. If the application code isn't ready, we usually
                    # get a 502 response, though, so I think this has to do with the
                    # preparation of the static endpoint. As evidence for this, they
                    # seem to occur after a rapid deploy->stop->deploy combination
                    # on the same endpoint. So we are blocking for up to a minute here
                    # to wait for the endpoint to be established. If we let requests
                    # handle the redirect it would quickly reach its redirect limit.
                    if redirects == 30:
                        raise AEUnexpectedResponseError("Too many self-redirects", method, url, **kwargs)
                    redirects += 1
                    time.sleep(2)
                else:
                    # In this case we are likely being redirected to auth to retrieve
                    # a cookie for the endpoint session itself. We will want to save
                    # this to avoid having to retrieve it every time. No need to sleep
                    # here since this is not an identical redirect
                    do_save = True
                    redirects = 0
                url = url2
                method = "get"
            elif allow_retry and (response.status_code == 401 or self._is_login(response)):
                self.authorize()
                if self.password is not None:
                    allow_retry = False
                redirects = 0
            elif response.status_code >= 400:
                raise AEUnexpectedResponseError(response, method, url, **kwargs)
            else:
                break
        if format == "response":
            return response
        if len(response.content) == 0:
            return None
        if format == "blob":
            return response.content
        if format == "text":
            return response.text
        if "json" in response.headers["content-type"]:
            return response.json()
        return response.text

    def _get(self, endpoint, **kwargs):
        return self._api("get", endpoint, **kwargs)

    def _delete(self, endpoint, **kwargs):
        return self._api("delete", endpoint, **kwargs)

    def _post(self, endpoint, **kwargs):
        return self._api("post", endpoint, **kwargs)

    def _head(self, endpoint, **kwargs):
        return self._api("head", endpoint, **kwargs)

    def _put(self, endpoint, **kwargs):
        return self._api("put", endpoint, **kwargs)

    def _patch(self, endpoint, **kwargs):
        return self._api("patch", endpoint, **kwargs)

    def _connect(self, password: str) -> None:
        # def _connect(self, password: Union[str, Any]) -> None:
        # TODO: impersonation does not yet function
        # It is left here a reminder ...
        # This will be needed for accessing service account private deployment tokens
        # if not isinstance(password, str):
        #     self.session.cookies = password.impersonate(self.username)
        # else:

        params: dict = {
            "client_id": "anaconda-platform",
            "scope": "openid",
            "response_type": "code",
            "redirect_uri": f"https://{self.hostname}/login",
        }
        url: str = f"https://{self.hostname}/auth/realms/AnacondaPlatform/protocol/openid-connect/auth"
        resp: Response = self.session.get(url, params=params)
        match: Optional[Match[AnyStr]] = re.search(r'<form id="kc-form-login".*?action="([^"]*)"', resp.text, re.M)
        if not match:
            # Already logged in, apparently?
            return
        data: dict = {"username": self.username, "password": password}
        resp = self.session.post(match.groups()[0].replace("&amp;", "&"), data=data)
        if "Invalid username or password." in resp.text:
            self.session.cookies.clear()

    def get_records(self, type: str) -> list[AERecordProject]:
        records_raw: list[dict] = self._api(method="get", endpoint=type)
        records: list[AERecordProject] = []
        for record_dict in records_raw:
            records.append(AERecordProject.parse_obj(record_dict))
        return records
