import re
import sys
import time
from abc import abstractmethod
from datetime import datetime
from http.cookiejar import LWPCookieJar
from typing import Any, Optional, Union

import requests
from dateutil import parser
from requests import Session

from ...cluster.identifier import Identifier
from ...contracts.dto.base_model import BaseModel
from ...contracts.dto.error.ae_error import AEError
from ...contracts.dto.error.ae_unexpected_response_error import AEUnexpectedResponseError
from ..constants import _DTYPES, COLUMNS, IDENT_FILTERS
from .utils.empty_record_list import EmptyRecordList
from .utils.filter import filter_list_of_dicts, filter_vars, split_filter


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
    # password: Optional[Union[str, AEAdminSession]] = None
    password: Optional[Union[str, Any]] = None
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
                hostname: The FQDN of the AE5 _login
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
        password = self.password
        self._connect(password)
        if self._connected():
            self._set_header()

    def disconnect(self):
        self._disconnect()
        self.session.headers.clear()
        self.session.cookies.clear()

    def _filter_records(self, filter, records):
        if not filter or not records:
            return records
        rec0 = records[0]
        records = filter_list_of_dicts(records, filter)
        if not records:
            records = EmptyRecordList(rec0["_record_type"], rec0)
        return records

    def _should_be_one(self, matches, filter, quiet):
        if isinstance(matches, dict) or matches is None:
            return matches
        if len(matches) == 1:
            return matches[0]
        if quiet:
            return None
        if matches:
            record_type = matches[0]["_record_type"]
        else:
            record_type = getattr(matches, "_record_type", "record")
        pfx = "Multiple" if len(matches) else "No"
        if isinstance(filter, (list, tuple)):
            filter = ",".join(filter)
        istr = record_type.replace("_", " ") + "s"
        msg = f"{pfx} {istr} found matching {filter}"
        if matches:
            if Identifier.has_prefix(record_type + "s"):
                matches = [str(Identifier.from_record(r)) for r in matches]
            else:
                vars = filter_vars(filter)
                matches = [",".join(f"{k}={r[k]}" for k in vars) for r in matches]
            msg += ":\n  - " + "\n  - ".join(matches)
        raise AEError(msg)

    def _fix_records(self, record_type, records, filter=None, **kwargs):
        pre = f"_pre_{record_type}"
        if isinstance(records, dict) and "data" in records:
            records = records["data"]
        is_single = isinstance(records, dict)
        if is_single:
            records = [records]
        if hasattr(self, pre):
            records = getattr(self, pre)(records)
        for rec in records:
            rec["_record_type"] = record_type
        if not records:
            records = EmptyRecordList(record_type)
        if records and filter:
            prefilt, postfilt = split_filter(filter, records[0])
            records = self._filter_records(prefilt, records)
        post = f"_post_{record_type}"
        if hasattr(self, post):
            records = getattr(self, post)(records, **kwargs)
        if records and filter:
            records = self._filter_records(postfilt, records)
        if is_single:
            return records[0] if records else None
        return records

    def ident_record(self, record_type, ident, quiet=False, **kwargs):
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

    def _format_table(self, response, columns):
        is_series = isinstance(response, dict)
        rlist = [response] if is_series else response
        csrc = list(rlist[0]) if rlist else getattr(response, "_columns", ())
        columns = [c.lstrip("?") for c in (columns or ())]
        cdst = [c for c in columns if c in csrc]
        cdst.extend(c for c in csrc if c not in columns and not c.startswith("_"))
        cdst.extend(c for c in csrc if c not in columns and c.startswith("_") and c != "_record_type")
        if "_record_type" in csrc:
            cdst.append("_record_type")
        for col in cdst:
            if col in _DTYPES:
                dtype = _DTYPES[col]
                if dtype == "datetime":
                    for rec in rlist:
                        if rec.get(col):
                            try:
                                rec[col] = parser.isoparse(rec[col])
                            except ValueError:
                                pass
                elif dtype.startswith("timestamp"):
                    incr = dtype.rsplit("/", 1)[1]
                    fact = 1000.0 if incr == "ms" else 1.0
                    for rec in rlist:
                        if rec.get(col):
                            rec[col] = datetime.fromtimestamp(rec[col] / fact)
        result = [tuple(rec.get(k) for k in cdst) for rec in rlist]
        if is_series:
            result = list(zip(cdst, result[0]))
            cdst = ["field", "value"]
        return (result, cdst)

    def _format_response(self, response, format, columns=None, record_type=None):
        if not isinstance(response, (list, dict)):
            if response is not None and format == "table":
                raise AEError("Response is not a tabular format")
            return response
        rlist = [response] if isinstance(response, dict) else response
        if record_type is not None:
            for rec in rlist:
                rec["_record_type"] = record_type
        if format not in ("table", "tableif", "dataframe", "_dataframe"):
            return response
        if record_type is None:
            if rlist and "_record_type" in rlist[0]:
                record_type = rlist[0]["_record_type"]
            else:
                record_type = getattr(response, "_record_type", None)
        if columns is None and record_type is not None:
            columns = COLUMNS.get(record_type, ())
        records, columns = self._format_table(response, columns)
        if format in ("dataframe", "_dataframe"):
            try:
                if format == "_dataframe":
                    raise ImportError
                import pandas as pd
            except ImportError:
                raise ImportError('Pandas must be installed in order to use format="dataframe"')
            return pd.DataFrame(records, columns=columns)
        return records, columns

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

    def api(self, method, endpoint, **kwargs):
        format = kwargs.pop("format", None)
        response = self._api(method, endpoint, **kwargs)
        return self._format_response(response, format=format)

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

    def _connect(self, password):
        # if isinstance(password, AEAdminSession):
        if not isinstance(password, str):
            self.session.cookies = password.impersonate(self.username)
        else:
            params = {
                "client_id": "anaconda-platform",
                "scope": "openid",
                "response_type": "code",
                "redirect_uri": f"https://{self.hostname}/login",
            }
            url = f"https://{self.hostname}/auth/realms/AnacondaPlatform/protocol/openid-connect/auth"
            resp = self.session.get(url, params=params)
            match = re.search(r'<form id="kc-form-login".*?action="([^"]*)"', resp.text, re.M)
            if not match:
                # Already logged in, apparently?
                return
            data = {"username": self.username, "password": password}
            resp = self.session.post(match.groups()[0].replace("&amp;", "&"), data=data)
            if "Invalid username or password." in resp.text:
                self.session.cookies.clear()
