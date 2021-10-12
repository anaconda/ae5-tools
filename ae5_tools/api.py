import requests
import time
import io
import re
import os
import sys
import json
import webbrowser
from os.path import basename, abspath, isfile, isdir, join
from fnmatch import fnmatch
from datetime import datetime
from dateutil import parser
import getpass
from tempfile import TemporaryDirectory
import tarfile
from pprint import pprint

from anaconda_project.project import Project

from .config import config
from .filter import filter_vars, split_filter, filter_list_of_dicts
from .identifier import Identifier
from .docker import get_dockerfile, get_condarc
from .docker import build_image
from .archiver import create_tar_archive
from .k8s.client import AE5K8SLocalClient, AE5K8SRemoteClient
from .git import install_prepush

from http.cookiejar import LWPCookieJar
from requests.packages import urllib3
import subprocess


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Maximum page size in keycloak
KEYCLOAK_PAGE_MAX = int(os.environ.get('KEYCLOAK_PAGE_MAX', '1000'))
# Maximum number of ids to pass through json body to the k8s endpoint
K8S_JSON_LIST_MAX = int(os.environ.get('K8S_JSON_LIST_MAX', '100'))

# Default subdomain for kubectl service
DEFAULT_K8S_ENDPOINT = 'k8s'

K8S_COLUMNS = ('phase', 'since', 'rst', 'usage/mem', 'usage/cpu', 'usage/gpu', 'changes', 'modified', 'node')

# Column labels prefixed with a '?' are not included in an initial empty record list.
# For instance, if the --collaborators flag is not set, then projects do not include a
# "collaborators" column. This allows us to provide a consistent header for record outputs
# even when the list is empty.
COLUMNS = {
    'project': ['name', 'owner', '?collaborators', 'editor', 'resource_profile', 'id', 'created', 'updated', 'project_create_status', 's3_bucket', 's3_path', 'git_server', 'repository', 'repo_owned', 'git_repos', 'repo_url', 'url'],
    'revision': ['name', 'latest', 'owner', 'commands', 'created', 'updated', 'id', 'url'],
    'command': ['id', 'supports_http_options', 'unix', 'windows', 'env_spec'],
    'collaborator': ['id', 'permission', 'type', 'first_name', 'last_name', 'email'],
    'session': ['name', 'owner', '?usage/mem', '?usage/cpu', '?usage/gpu', '?modified', '?node', '?rst', 'resource_profile', 'id', 'created', 'updated', 'state', '?phase', '?since', '?rst', 'project_id', 'session_name', 'project_branch', 'iframe_hosts', 'url', 'project_url'],
    'resource_profile': ['name', 'description', 'cpu', 'memory', 'gpu', 'id'],
    'editor': ['name', 'id', 'is_default', 'packages'],
    'sample': ['name', 'id', 'is_template', 'is_default', 'description', 'download_url', 'owner', 'created', 'updated'],
    'deployment': ['endpoint', 'name', 'owner', '?usage/mem', '?usage/cpu', '?usage/gpu', '?node', '?rst', 'public', '?collaborators', 'command', 'eevision', 'resource_profile', 'id', 'created', 'updated', 'state', '?phase', '?since', '?rst', 'project_id', 'project_name', 'project_owner'],
    'job': ['name', 'owner', 'command', 'revision', 'resource_profile', 'id', 'created', 'updated', 'state', 'project_id', 'project_name'],
    'run': ['name', 'owner', 'command', 'revision', 'resource_profile', 'id', 'created', 'updated', 'state', 'project_id', 'project_name'],
    'branch': ['branch', 'sha1'],
    'change': ['path', 'change_type', 'modified', 'conflicted', 'id'],
    'user': ['username', 'firstName', 'lastName', 'lastLogin', 'email', 'id'],
    'activity': ['type', 'status', 'message', 'done', 'owner', 'id', 'description', 'created', 'updated'],
    'endpoint': ['id', 'owner', 'name', 'project_name', 'deployment_id', 'project_id', 'project_url'],
    'pod': ['name', 'owner', 'type', 'usage/mem', 'usage/cpu', 'usage/gpu', 'node', 'rst', 'modified', 'phase', 'since', 'resource_profile', 'id', 'project_id'],
}

IDENT_FILTERS = {
    'endpoint': 'id={value}',
    'editor': 'name={value}|id={value}',
    'node': 'name={value}',
    'resource_profile': 'name={value}',
    'sample': 'name={value}|id={value}',
    'collaborator': 'id={value}',
    'user': 'username={value}|id={value}'
}

_DTYPES = {'created': 'datetime', 'updated': 'datetime',
           'since': 'datetime', 'mtime': 'datetime', 'timestamp': 'datetime',
           'createdTimestamp': 'timestamp/ms', 'notBefore': 'timestamp/s',
           'lastLogin': 'timestamp/ms', 'time': 'timestamp/ms'}


class EmptyRecordList(list):
    def __init__(self, record_type, columns=None):
        self._record_type = record_type
        if columns is not None:
            self._columns = list(columns)
        else:
            self._columns = list(c for c in COLUMNS.get(record_type, ()) if not c.startswith('?'))
        super(EmptyRecordList, self).__init__()
    def __str__(self):
        return f'EmptyRecordList: record_type={self._record_type}\n  - columns: ' + ','.join(self._columns)


class AEException(RuntimeError):
    pass


class AEUnexpectedResponseError(AEException):
    def __init__(self, response, method, url, **kwargs):
        if isinstance(response, str):
            msg = [f'Unexpected response: {response}']
        else:
            msg = [f'Unexpected response: {response.status_code} {response.reason}',
                   f'  {method.upper()} {url}']
            if response.headers:
                msg.append(f'  headers: {response.headers}')
            if response.text:
                msg.append(f'  text: {response.text}')
        if 'params' in kwargs:
            msg.append(f'  params: {kwargs["params"]}')
        if 'data' in kwargs:
            msg.append(f'  data: {kwargs["data"]}')
        if 'json' in kwargs:
            msg.append(f'  json: {kwargs["json"]}')
        super(AEUnexpectedResponseError, self).__init__('\n'.join(msg))
    pass


class AESessionBase(object):
    '''Base class for AE5 API interactions.'''

    def __init__(self, hostname, username, password, prefix, persist):
        '''Base class constructor.

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
        '''
        if not hostname or not username:
            raise ValueError('Must supply hostname and username')
        self.hostname = hostname
        self.username = username
        self.password = password
        self.persist = persist
        self.prefix = prefix.lstrip('/')
        self.session = requests.Session()
        self.session.verify = False
        self.session.cookies = LWPCookieJar()
        if self.persist:
            self._load()
        self.connected = self._connected()
        if self.connected:
            self._set_header()

    @staticmethod
    def _auth_message(msg, nl=True):
        print(msg, file=sys.stderr, end='\n' if nl else '')
        sys.stderr.flush()

    @staticmethod
    def _password_prompt(key, last_valid=True):
        cls = AESessionBase
        if not last_valid:
            cls._auth_message('Invalid username or password; please try again.')
        while True:
            cls._auth_message(f'Password for {key}: ', False)
            password = getpass.getpass('')
            if password:
                return password
            cls._auth_message('Must supply a password.')

    def __del__(self):
        # Try to be a good citizen and shut down the active session.
        # But fail silently if it does not work. In particular, if this
        # destructor is called too late in the shutdown process, the call
        # to requests will fail with an ImportError.
        if sys.meta_path is not None and not getattr(self, 'persist', True) and self.connected:
            try:
                self.disconnect()
            except Exception:
                pass

    def _is_login(self, response):
        pass

    def authorize(self):
        key = f'{self.username}@{self.hostname}'
        need_password = self.password is None
        last_valid = True
        while True:
            if need_password:
                password = self._password_prompt(key, last_valid)
            else:
                password = self.password
            self._connect(password)
            if self._connected():
                break
            if not need_password:
                raise AEException('Invalid username or password.')
            last_valid = False
        if self._connected():
            self.connected = True
            self._set_header()
            if self.persist:
                self._save()

    def disconnect(self):
        self._disconnect()
        self.session.headers.clear()
        self.session.cookies.clear()
        if self.persist:
            self._save()
        self.connected = False

    def _filter_records(self, filter, records):
        if not filter or not records:
            return records
        rec0 = records[0]
        records = filter_list_of_dicts(records, filter)
        if not records:
            records = EmptyRecordList(rec0['_record_type'], rec0)
        return records

    def _should_be_one(self, matches, filter, quiet):
        if isinstance(matches, dict) or matches is None:
            return matches
        if len(matches) == 1:
            return matches[0]
        if quiet:
            return None
        if matches:
            record_type = matches[0]['_record_type']
        else:
            record_type = getattr(matches, '_record_type', 'record')
        pfx = 'Multiple' if len(matches) else 'No'
        if isinstance(filter, (list, tuple)):
            filter = ','.join(filter)
        istr = record_type.replace('_', ' ') + 's'
        msg = f'{pfx} {istr} found matching {filter}'
        if matches:
            if Identifier.has_prefix(record_type + 's'):
                matches = [str(Identifier.from_record(r)) for r in matches]
            else:
                vars = filter_vars(filter)
                matches = [','.join(f'{k}={r[k]}' for k in vars) for r in matches]
            msg += ':\n  - ' + '\n  - '.join(matches)
        raise AEException(msg)

    def _fix_records(self, record_type, records, filter=None, **kwargs):
        pre = f'_pre_{record_type}'
        if isinstance(records, dict) and 'data' in records:
            records = records['data']
        is_single = isinstance(records, dict)
        if is_single:
            records = [records]
        if hasattr(self, pre):
            records = getattr(self, pre)(records)
        for rec in records:
            rec['_record_type'] = record_type
        if not records:
            records = EmptyRecordList(record_type)
        if records and filter:
            prefilt, postfilt = split_filter(filter, records[0])
            records = self._filter_records(prefilt, records)
        post = f'_post_{record_type}'
        if hasattr(self, post):
            records = getattr(self, post)(records, **kwargs)
        if records and filter:
            records = self._filter_records(postfilt, records)
        if is_single:
            return records[0] if records else None
        return records

    def _ident_record(self, record_type, ident, quiet=False, **kwargs):
        if isinstance(ident, dict) and ident.get('_record_type', '') == record_type:
            return ident
        itype = record_type + 's'
        if isinstance(ident, Identifier):
            filter = ident.project_filter(itype=itype, ignore_revision=True)
        elif isinstance(ident, tuple):
            ident, filter = ','.join(ident), ident
        elif record_type in IDENT_FILTERS:
            ident = filter = IDENT_FILTERS[record_type].format(value=ident)
        else:
            ident = Identifier.from_string(ident, itype)
            filter = ident.project_filter(itype=itype, ignore_revision=True)
        matches = getattr(self, f'{record_type}_list')(filter=filter, **kwargs)
        return self._should_be_one(matches, filter, quiet)

    def _format_table(self, response, columns):
        is_series = isinstance(response, dict)
        rlist = [response] if is_series else response
        csrc = list(rlist[0]) if rlist else getattr(response, '_columns', ())
        columns = [c.lstrip('?') for c in (columns or ())]
        cdst = [c for c in columns if c in csrc]
        cdst.extend(c for c in csrc if c not in columns and not c.startswith('_'))
        cdst.extend(c for c in csrc if c not in columns and c.startswith('_') and c != '_record_type')
        if '_record_type' in csrc:
            cdst.append('_record_type')
        for col in cdst:
            if col in _DTYPES:
                dtype = _DTYPES[col]
                if dtype == 'datetime':
                    for rec in rlist:
                        if rec.get(col):
                            try:
                                rec[col] = parser.isoparse(rec[col])
                            except ValueError:
                                pass
                elif dtype.startswith('timestamp'):
                    incr = dtype.rsplit('/', 1)[1]
                    fact = 1000.0 if incr == 'ms' else 1.0
                    for rec in rlist:
                        if rec.get(col):
                            rec[col] = datetime.fromtimestamp(rec[col] / fact)
        result = [tuple(rec.get(k) for k in cdst) for rec in rlist]
        if is_series:
            result = list(zip(cdst, result[0]))
            cdst = ['field', 'value']
        return (result, cdst)

    def _format_response(self, response, format, columns=None, record_type=None):
        if not isinstance(response, (list, dict)):
            if response is not None and format == 'table':
                raise AEException('Response is not a tabular format')
            return response
        rlist = [response] if isinstance(response, dict) else response
        if record_type is not None:
            for rec in rlist:
                rec['_record_type'] = record_type
        if format not in ('table', 'tableif', 'dataframe', '_dataframe'):
            return response
        if record_type is None:
            if rlist and '_record_type' in rlist[0]:
                record_type = rlist[0]['_record_type']
            else:
                record_type = getattr(response, '_record_type', None)
        if columns is None and record_type is not None:
            columns = COLUMNS.get(record_type, ())
        records, columns = self._format_table(response, columns)
        if format in ('dataframe', '_dataframe'):
            try:
                if format == '_dataframe':
                    raise ImportError
                import pandas as pd
            except ImportError:
                raise ImportError('Pandas must be installed in order to use format="dataframe"')
            return pd.DataFrame(records, columns=columns)
        return records, columns

    def _api(self, method, endpoint, **kwargs):
        format = kwargs.pop('format', None)
        subdomain = kwargs.pop('subdomain', None)
        isabs, endpoint = endpoint.startswith('/'), endpoint.lstrip('/')
        if subdomain:
            subdomain += '.'
            isabs = True
        else:
            subdomain = ''
        if not isabs:
            endpoint = f'{self.prefix}/{endpoint}'
        url = f'https://{subdomain}{self.hostname}/{endpoint}'
        do_save = False
        allow_retry = True
        if not self.connected:
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
                    raise AEUnexpectedResponseError('Unable to connect', method, url, **kwargs)
                retries += 1
                time.sleep(2)
                continue
            except requests.exceptions.Timeout:
                raise AEUnexpectedResponseError('Connection timeout', method, url, **kwargs)
            if 300 <= response.status_code < 400:
                # Redirection here happens for two reasons, described below. We
                # handle them ourselves to provide better behavior than requests.
                url2 = response.headers['location'].rstrip()
                if url2.startswith('/'):
                    url2 = f'https://{subdomain}{self.hostname}{url2}'
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
                        raise AEUnexpectedResponseError('Too many self-redirects', method, url, **kwargs)
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
                method = 'get'
            elif allow_retry and (response.status_code == 401 or self._is_login(response)):
                self.authorize()
                if self.password is not None:
                    allow_retry = False
                redirects = 0
            elif response.status_code >= 400:
                raise AEUnexpectedResponseError(response, method, url, **kwargs)
            else:
                if do_save and self.persist:
                    self._save()
                break
        if format == 'response':
            return response
        if len(response.content) == 0:
            return None
        if format == 'blob':
            return response.content
        if format == 'text':
            return response.text
        if 'json' in response.headers['content-type']:
            return response.json()
        return response.text

    def api(self, method, endpoint, **kwargs):
        format = kwargs.pop('format', None)
        response = self._api(method, endpoint, **kwargs)
        return self._format_response(response, format=format)

    def _get(self, endpoint, **kwargs):
        return self._api('get', endpoint, **kwargs)

    def _delete(self, endpoint, **kwargs):
        return self._api('delete', endpoint, **kwargs)

    def _post(self, endpoint, **kwargs):
        return self._api('post', endpoint, **kwargs)

    def _head(self, endpoint, **kwargs):
        return self._api('head', endpoint, **kwargs)

    def _put(self, endpoint, **kwargs):
        return self._api('put', endpoint, **kwargs)

    def _patch(self, endpoint, **kwargs):
        return self._api('patch', endpoint, **kwargs)


class AEUserSession(AESessionBase):
    def __init__(self, hostname, username, password=None, persist=True, k8s_endpoint=None):
        self._filename = os.path.join(config._path, 'cookies', f'{username}@{hostname}')
        super(AEUserSession, self).__init__(hostname, username, password=password,
                                            prefix='api/v2', persist=persist)
        self._k8s_endpoint = k8s_endpoint or os.environ.get('AE5_K8S_ENDPOINT') or 'k8s'
        self._k8s_client = None

    def _k8s(self, method, *args, **kwargs):
        quiet = kwargs.pop('quiet', False)
        if self._k8s_client is None and self._k8s_endpoint is not None:
            if self._k8s_endpoint.startswith('ssh:'):
                username = self._k8s_endpoint[4:]
                self._k8s_client = AE5K8SLocalClient(self.hostname, username)
            else:
                self._k8s_client = AE5K8SRemoteClient(self, self._k8s_endpoint)
            estr = self._k8s_client.error()
            if estr:
                del self._k8s_client
                self._k8s_endpoint = self._k8s_client = None
                msg = ['Error establishing k8s connection:']
                msg.extend('  ' + x for x in estr.splitlines())
                raise AEException('\n'.join(msg))
        if self._k8s_client is None:
            raise AEException('No k8s connection available')
        return getattr(self._k8s_client, method)(*args, **kwargs)

    def _set_header(self):
        s = self.session
        for cookie in s.cookies:
            if cookie.name == '_xsrf':
                s.headers['x-xsrftoken'] = cookie.value
                break

    def _load(self):
        s = self.session
        if os.path.exists(self._filename):
            s.cookies.load(self._filename, ignore_discard=True)
            os.utime(self._filename)

    def _connected(self):
        return any(c.name == '_xsrf' for c in self.session.cookies)

    def _is_login(self, resp):
        if resp.status_code == 200:
            ctype = resp.headers['content-type']
            if ctype.startswith('text/html'):
                return bool(re.search(r'<form id="kc-form-login"', resp.text, re.M))

    def _connect(self, password):
        if isinstance(password, AEAdminSession):
            self.session.cookies = password.impersonate(self.username)
        else:
            params = {'client_id': 'anaconda-platform',
                      'scope': 'openid',
                      'response_type': 'code',
                      'redirect_uri': f'https://{self.hostname}/login'}
            url = f'https://{self.hostname}/auth/realms/AnacondaPlatform/protocol/openid-connect/auth'
            resp = self.session.get(url, params=params)
            match = re.search(r'<form id="kc-form-login".*?action="([^"]*)"', resp.text, re.M)
            if not match:
                # Already logged in, apparently?
                return
            data = {'username': self.username, 'password': password}
            resp = self.session.post(match.groups()[0].replace('&amp;', '&'), data=data)
            if 'Invalid username or password.' in resp.text:
                self.session.cookies.clear()


    def _disconnect(self):
        # This will actually close out the session, so even if the cookie had
        # been captured for use elsewhere, it would no longer be useful.
        self._get('/logout')

    def _save(self):
        os.makedirs(os.path.dirname(self._filename), mode=0o700, exist_ok=True)
        self.session.cookies.save(self._filename, ignore_discard=True)
        os.chmod(self._filename, 0o600)

    def _api_records(self, method, endpoint, filter=None, **kwargs):
        record_type = kwargs.pop('record_type', None)
        api_kwargs = kwargs.pop('api_kwargs', None) or {}
        retry_if_empty = kwargs.pop('retry_if_empty', False)
        if not record_type:
            record_type = endpoint.rsplit('/', 1)[-1].rstrip('s')
        for attempt in range(20):
            records = self._api(method, endpoint, **api_kwargs)
            if records or not retry_if_empty:
                break
            time.sleep(0.25)
        else:
            raise AEException(f'Unexpected empty {record_type} recordset')
        return self._fix_records(record_type, records, filter, **kwargs)

    def _get_records(self, endpoint, filter=None, **kwargs):
        return self._api_records('get', endpoint, filter=filter, **kwargs)

    def _post_record(self, endpoint, filter=None, **kwargs):
        return self._api_records('post', endpoint, filter=filter, **kwargs)

    def _post_project(self, records, collaborators=False):
        if collaborators:
            self._join_collaborators('projects', records)
        return records

    def project_list(self, filter=None, collaborators=False, format=None):
        records = self._get_records('projects', filter, collaborators=collaborators)
        return self._format_response(records, format=format)

    def project_info(self, ident, collaborators=False, format=None, quiet=False, retry=False):
        # Retry loop added because project creation is now so fast that the API
        # often needs time to catch up before it "sees" the new project. We only
        # use the retry loop in project creation commands for that reason.
        while True:
            try:
                record = self._ident_record('project', ident, collaborators=collaborators, quiet=quiet)
                break
            except AEException as exc:
                if not retry or not str(exc).startswith('No projects found matching id'):
                    raise
                time.sleep(0.25)
        return self._format_response(record, format=format)

    def project_patch(self, ident, format=None, **kwargs):
        prec = self._ident_record('project', ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            id = prec["id"]
            self._patch(f'projects/{id}', json=data)
            prec = self._ident_record('project', id)
        return self._format_response(prec, format=format)

    def project_delete(self, ident, format=None):
        id = self._ident_record('project', ident)['id']
        self._delete(f'projects/{id}')

    def project_collaborator_list(self, ident, filter=None, format=None):
        id = self._ident_record('project', ident)['id']
        response = self._get_records(f'projects/{id}/collaborators', filter)
        return self._format_response(response, format=format)

    def project_collaborator_info(self, ident, userid, quiet=False, format=None):
        filter = f'id={userid}'
        response = self.project_collaborator_list(ident, filter=filter)
        response = self._should_be_one(response, filter, quiet)
        return self._format_response(response, format=format)

    def project_collaborator_list_set(self, ident, collabs, format=None):
        id = self._ident_record('project', ident)['id']
        result = self._put(f'projects/{id}/collaborators', json=collabs)
        if result['action']['error'] or 'collaborators' not in result:
            raise AEException(f'Unexpected error adding collaborator: {result}')
        result = self._fix_records('collaborator', result['collaborators'])
        return self._format_response(result, format=format)

    def project_collaborator_add(self, ident, userid, group=False, read_only=False, format=None):
        prec = self._ident_record('project', ident)
        collabs = self.project_collaborator_list(prec)
        cmap = {c['id']: (c['type'], c['permission']) for c in collabs}
        if not isinstance(userid, tuple):
            userid = userid,
        tp = ('group' if group else 'user', 'r' if read_only else 'rw')
        nmap = {k: tp for k in userid if k not in cmap}
        nmap.update((k, v) for k, v in cmap.items() if k not in userid or v == tp)
        if nmap != cmap:
            collabs = [{'id': k, 'type': t, 'permission': p}
                       for k, (t, p) in nmap.items()]
            collabs = self.project_collaborator_list_set(prec, collabs)
        if any(k not in nmap for k in userid):
            nmap.update((k, tp) for k in userid)
            collabs = [{'id': k, 'type': t, 'permission': p}
                       for k, (t, p) in nmap.items()]
            collabs = self.project_collaborator_list_set(prec, collabs)
        return self._format_response(collabs, format=format)

    def project_collaborator_remove(self, ident, userid, format=None):
        prec = self._ident_record('project', ident)
        collabs = self.project_collaborator_list(prec)
        if not isinstance(userid, tuple):
            userid = userid,
        missing = set(userid) - set(c['id'] for c in collabs)
        if missing:
            missing = ', '.join(missing)
            raise AEException(f'Collaborator(s) not found: {missing}')
        collabs = [c for c in collabs if c['id'] not in userid]
        return self.project_collaborator_list_set(prec, collabs, format=format)

    def _pre_resource_profile(self, response):
        for profile in response:
            profile['description'], params = profile['description'].rsplit(' (', 1)
            for param in params.rstrip(')').split(', '):
                k, v = param.split(': ', 1)
                profile[k.lower()] = v
            if 'gpu' not in profile:
                profile['gpu'] = 0
        return response

    def resource_profile_list(self, filter=None, format=None):
        response = self._get('projects/actions', params={'q': 'create_action'})
        response = response[0]['resource_profiles']
        response = self._fix_records('resource_profile', response, filter=filter)
        return self._format_response(response, format=format)

    def resource_profile_info(self, name, format=None, quiet=False):
        response = self._ident_record('resource_profile', name, quiet)
        return self._format_response(response, format=format)

    def _pre_editor(self, response):
        for rec in response:
            rec['packages'] = ', '.join(rec['packages'])
        return response

    def editor_list(self, filter=None, format=None):
        response = self._get('projects/actions', params={'q': 'create_action'})
        response = response[0]['editors']
        response = self._fix_records('editor', response, filter=filter)
        return self._format_response(response, format=format)

    def editor_info(self, name, format=None, quiet=False):
        response = self._ident_record('editor', name, quiet)
        return self._format_response(response, format=format)

    def _pre_sample(self, records):
        for record in records:
            if record.get('is_default'):
                record['is_default'] = False
        first_template = None
        found_default = False
        for record in records:
            record['is_default'] = bool(not found_default and record.get('is_template') and record.get('is_default'))
            record.setdefault('is_template', False)
            first_template = first_template or record
            found_default = found_default or record['is_default']
        if not found_default and first_template:
            first_template['is_default'] = True
        return records

    def sample_list(self, filter=None, format=None):
        records = self._get('template_projects') + self._get('sample_projects')
        response = self._fix_records('sample', records, filter)
        return self._format_response(response, format=format)

    def sample_info(self, ident, format=None, quiet=False):
        response = self._ident_record('sample', ident, quiet)
        return self._format_response(response, format=format)

    def sample_clone(self, ident, name=None, tag=None,
                     make_unique=None, wait=True, format=None):
        record = self._ident_record('sample', ident)
        if name is None:
            name = record['name']
            if make_unique is None:
                make_unique = True
        return self.project_create(record['download_url'], name=name, tag=tag,
                                   make_unique=make_unique, wait=wait, format=format)

    def project_sessions(self, ident, format=None):
        id = self._ident_record('project', ident)["id"]
        response = self._get_records(f'projects/{id}/sessions')
        return self._format_response(response, format=format)

    def project_deployments(self, ident, format=None):
        id = self._ident_record('project', ident)["id"]
        response = self._get_records(f'projects/{id}/deployments')
        return self._format_response(response, format=format)

    def project_jobs(self, ident, format=None):
        id = self._ident_record('project', ident)["id"]
        response = self._get_records(f'projects/{id}/jobs')
        return self._format_response(response, format=format)

    def project_runs(self, ident, format=None):
        id = self._ident_record('project', ident)["id"]
        response = self._get_records(f'projects/{id}/runs')
        return self._format_response(response, format=format)

    def project_activity(self, ident, limit=None, all=False, latest=False, format=None):
        id = self._ident_record('project', ident)["id"]
        if all and latest:
            raise AEException('Cannot specify both all=True and latest=True')
        elif limit is None:
            limit = 1 if latest else (0 if all else 10)
        elif all and limit > 0:
            raise AEException(f'Cannot specify both all=True and limit={limit}')
        elif latest and limit > 1:
            raise AEException(f'Cannot specify both latest=True and limit={limit}')
        elif limit <= 0:
            limit = 999999
        api_kwargs = {'params': {'sort': '-updated', 'page[size]': limit}}
        response = self._get_records(f'projects/{id}/activity', api_kwargs=api_kwargs)
        if latest:
            response = response[0]
        return self._format_response(response, format=format)

    def _pre_revision(self, records):
        first = True
        for rec in records:
            rec['project_id'] = 'a0-' + rec['url'].rsplit('/', 3)[1]
            rec['latest'], first = first, False
            commands = rec['commands']
            for c in commands:
                c['_record_type'] = 'command'
            rec['commands'] = ', '.join(c['id'] for c in commands)
            rec['_commands'] = commands
        return records

    def _post_revision(self, records, project=None):
        for rec in records:
            rec['_project'] = project
        return records

    def _revisions(self, ident, filter=None, latest=False, single=False, quiet=False):
        if isinstance(ident, dict):
            revision = ident.get('_revision')
        elif isinstance(ident, tuple):
            revision = ''.join(r[9:] for r in ident if r.startswith('revision='))
            ident = tuple(r for r in ident if not r.startswith('revision='))
        else:
            if isinstance(ident, str):
                ident = Identifier.from_string(ident)
            revision = ident.revision
        if revision == 'latest':
            latest = latest or True
            revision = None
        elif revision:
            latest = False
        prec = self._ident_record('project', ident, quiet=quiet)
        if prec is None:
            return None
        id = prec["id"]
        if not filter:
            filter = ()
        if latest:
            filter = (f'latest=True',) + filter
        elif revision and revision != '*':
            filter = (f'name={revision}',) + filter
        response = self._get_records(f'projects/{id}/revisions', filter=filter, project=prec, retry_if_empty=True)
        if latest == 'keep' and response:
            response[0]['name'] = 'latest'
        if single:
            response = self._should_be_one(response, filter, quiet)
        return response

    def _revision(self, ident, keep_latest=False, quiet=False):
        latest = 'keep' if keep_latest else True
        return self._revisions(ident, latest=latest, single=True, quiet=quiet)

    def revision_list(self, ident, filter=None, format=None):
        response = self._revisions(ident, filter, quiet=False)
        return self._format_response(response, format=format)

    def revision_info(self, ident, format=None, quiet=False):
        rrec = self._revision(ident, quiet=quiet)
        return self._format_response(rrec, format=format)

    def revision_commands(self, ident, format=None, quiet=False):
        rrec = self._revision(ident, quiet=quiet)
        return self._format_response(rrec['_commands'], format=format)

    def project_download(self, ident, filename=None, format=None):
        rrec = self._revision(ident, keep_latest=True)
        prec, rev = rrec['_project'], rrec['id']
        need_filename = not bool(filename)
        if need_filename:
            revdash = f'-{rrec["name"]}' if rrec['name'] != 'latest' else ''
            filename = f'{prec["name"]}{revdash}.tar.gz'
        response = self._get(f'projects/{prec["id"]}/revisions/{rev}/archive', format='blob')
        with open(filename, 'wb') as fp:
            fp.write(response)
        if need_filename:
            return filename

    def project_image(self, ident, command=None, condarc=None, dockerfile=None, debug=False, format=None):
        '''Build docker image'''
        rrec = self._revision(ident, keep_latest=True)
        prec, rev = rrec['_project'], rrec['id']
        name = prec['name'].replace(' ','').lower()
        owner = prec['owner'].replace('@','_at_')
        tag = f'{owner}/{name}:{rev}'

        dockerfile_contents = get_dockerfile(dockerfile)
        condarc_contents = get_condarc(condarc)

        if command:
            commands = [c['id'] for c in rrec['_commands']]
            if not commands:
                print('There are no configured commands in this project.')
                print('Remove the --command option to build the container anyway.')
                return
            if command in commands:
                dockerfile_contents += f'\nCMD anaconda-project run {command} --anaconda-project-port 8086'
            else:
                print(f'The command {command} is not one of the configured commands.')
                print('Available commands are:')
                for c in rrec['_commands']:
                    default = c.get('default', False)
                    if default:
                        print(f'  {c["id"]:15s} (default)')
                    else:
                        print(f'  {c["id"]:15s}')
                return
        else:
            default_cmd = [c["id"] for c in rrec['_commands'] if c.get('default')]
            if default_cmd:
                dockerfile_contents += f'\nCMD anaconda-project run {default_cmd[0]} --anaconda-project-port 8086'

        with TemporaryDirectory() as tempdir:
            with open(os.path.join(tempdir, 'Dockerfile'), 'w') as f:
                f.write(dockerfile_contents)

            with open(os.path.join(tempdir, 'condarc'), 'w') as f:
                f.write(condarc_contents)

            self.project_download(ident, filename=os.path.join(tempdir, 'project.tar.gz'))

            print('Starting image build. This may take several minutes.')
            build_image(tempdir, tag=tag, debug=debug)

    def _wait(self, response):
        index = 0
        id = response.get('project_id', response['id'])
        status = response['action']
        while not status['done'] and not status['error']:
            time.sleep(1)
            params = {'sort': '-updated', 'page[size]': index + 1}
            activity = self._get(f'projects/{id}/activity', params=params)
            try:
                status = next(s for s in activity['data'] if s['id'] == status['id'])
            except StopIteration:
                index = index + 1
        response['action'] = status

    def project_create(self, url, name=None, tag=None,
                       make_unique=None, wait=True, format=None):
        if not name:
            parts = urllib3.util.parse_url(url)
            name = basename(parts.path).split('.', 1)[0]
            if make_unique is None:
                make_unique = True
        params = {'name': name, 'source': url, 'make_unique': bool(make_unique)}
        if tag:
            params['tag'] = tag
        response = self._post_record('projects', api_kwargs={'json': params})
        if response.get('error'):
            raise RuntimeError('Error creating project: {}'.format(response['error']['message']))
        if wait:
            self._wait(response)
        if response['action']['error']:
            raise RuntimeError('Error processing creation: {}'.format(response['action']['message']))
        if wait:
            return self.project_info(response['id'], format=format, retry=True)

    def project_upload(self, project_archive, name, tag, wait=True, format=None):
        if not name:
            if type(project_archive) == bytes:
                raise RuntimeError('Project name must be supplied for binary input')
            name = basename(abspath(project_archive))
            for suffix in  ('.tar.gz', '.tar.bz2', '.tar.gz', '.zip', '.tgz', '.tbz', '.tbz2', '.tz2', '.txz'):
                if name.endswith(suffix):
                    name = name[:-len(suffix)]
                    break
        try:
            f = None
            if type(project_archive) == bytes:
                f = io.BytesIO(project_archive)
            elif not os.path.exists(project_archive):
                raise RuntimeError(f'File/directory not found: {project_archive}')
            elif not isdir(project_archive):
                f = open(project_archive, 'rb')
            elif not isfile(join(project_archive, 'anaconda-project.yml')):
                raise RuntimeError(f'Project directory must include anaconda-project.yml')
            else:
                f = io.BytesIO()
                create_tar_archive(project_archive, 'project', f)
                project_archive = project_archive + '.tar.gz'
            f.seek(0)
            data = {'name': name}
            if tag:
                data['tag'] = tag
            f = (project_archive, f)
            response = self._post_record('projects/upload', record_type='project',
                                         api_kwargs={'files': {b'project_file': f},
                                         'data': data})
        finally:
            if f is not None:
                f[1].close()
        if response.get('error'):
            raise RuntimeError('Error uploading project: {}'.format(response['error']['message']))
        if wait:
            self._wait(response)
        if response['action']['error']:
            raise RuntimeError('Error processing upload: {}'.format(response['action']['message']))
        if wait:
            return self.project_info(response['id'], format=format, retry=True)

    def project_clone(self, ident, directory="", format=None):
        extraheader = ''
        if self.hostname in ident["repo_url"]:
            repo_url = ident['repo_url']
            token = self._get_v1_token()
            extraheader = f' -c http.extraheader="AUTHORIZATION: bearer {token}" '
        if 'anaconda-enterprise-ap-git-storage' in ident['repo_url']:
            ## newer versions of ae5
            repo_url = f'https://{self.hostname}/platform/git/anaconda/{ident["repository"]}.git'
            token = self._get_v1_token()
            extraheader = f' -c http.extraheader="AUTHORIZATION: bearer {token}" '
        else:
            ## most likely external git
            repo_url = ident['repo_url']

        subprocess.check_call(f'git clone {extraheader} -c remote.origin.project={ident["id"]} {repo_url} {directory}',
                              shell=True)
        if not directory:
            directory = os.path.basename(ident["repo_url"]).split('.git')[0]
        install_prepush(directory)

    def _join_collaborators(self, what, response):
        if isinstance(response, dict):
            what, id = response['_record_type'], response['id']
            collabs = self._get_records(f'{what}s/{id}/collaborators')
            response['collaborators'] = ', '.join(c['id'] for c in collabs)
            response['_collaborators'] = collabs
        elif response:
            for rec in response:
                self._join_collaborators(what, rec)
        elif hasattr(response, '_columns'):
            response._columns.extend(('collaborators', '_collaborators'))

    def _join_k8s(self, record, changes=False):
        is_single = isinstance(record, dict)
        rlist = [record] if is_single else record
        if rlist:
            rlist2 = []
            # Limit the size of the input to pod_info to avoid 413 errors
            idchunks = [[r['id'] for r in rlist[k:k + K8S_JSON_LIST_MAX]]
                        for k in range(0, len(rlist), K8S_JSON_LIST_MAX)]
            record2 = sum((self._k8s('pod_info', ch) for ch in idchunks), [])
            for rec, rec2 in zip(rlist, record2):
                if not rec2:
                    continue
                rlist2.append(rec)
                rec['phase'] = rec2['phase']
                rec['since'] = rec2['since']
                rec['rst'] = rec2['restarts']
                rec['usage/mem'] = rec2['usage']['mem']
                rec['usage/cpu'] = rec2['usage']['cpu']
                rec['usage/gpu'] = rec2['usage']['gpu']
                if changes:
                    if 'changes' in rec2:
                        chg = rec2['changes']
                        chg = ','.join(chg['modified'] + chg['deleted'] + chg['added'])
                        rec['changes'] = chg
                        rec['modified'] = bool(chg)
                    else:
                        rec['modified'] = 'n/a'
                        rec['changes'] = ''
                rec['node'] = rec2['node']
                rec['_k8s'] = rec2
            if not rlist2:
                rlist2 = EmptyRecordList(rlist[0]['_record_type'], rlist[0])
            rlist = rlist2
        if not rlist and hasattr(rlist, '_columns'):
            rlist._columns.extend(('phase', 'since', 'rst', 'usage/mem', 'usage/cpu', 'usage/gpu'))
            if changes:
                rlist._columns.extend(('changes', 'modified'))
            rlist._columns.extend(('node', '_k8s'))
        return record if is_single else rlist

    def _pre_session(self, records):
        # The "name" value in an internal AE5 session record is nothing
        # more than the "id" value with the "a1-" stub removed. Not very
        # helpful, even if understandable.
        precs = {x['id']: x for x in self._get_records('projects')}
        for rec in records:
            pid = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
            prec = precs.get(pid, {})
            rec['session_name'] = rec['name']
            rec['name'] = prec['name']
            rec['project_id'] = pid
            rec['_project'] = prec
        return records

    def _post_session(self, records, k8s=False):
        if k8s:
            return self._join_k8s(records, changes=True)
        return records

    def session_list(self, filter=None, k8s=False, format=None):
        records = self._get_records('sessions', filter, k8s=k8s)
        return self._format_response(records, format, record_type='session')

    def session_info(self, ident, k8s=False, format=None, quiet=False):
        record = self._ident_record('session', ident, quiet=quiet, k8s=k8s)
        return self._format_response(record, format)

    def session_start(self, ident, editor=None, resource_profile=None, wait=True, open=False, frame=True, format=None):
        prec = self._ident_record('project', ident)
        id = prec['id']
        patches = {}
        if editor and prec['editor'] != editor:
            patches['editor'] = editor
        if resource_profile and prec['resource_profile'] != resource_profile:
            patches['resource_profile'] = resource_profile
        if patches:
            self._patch(f'projects/{id}', json=patches)
        response = self._post_record(f'projects/{id}/sessions')
        if response.get('error'):
            raise RuntimeError('Error starting project: {}'.format(response['error']['message']))
        if wait or open:
            self._wait(response)
        if response['action'].get('error'):
            raise RuntimeError('Error completing session start: {}'.format(response['action']['message']))
        if open:
            self.session_open(response, frame)
        return self._format_response(response, format=format)

    def session_stop(self, ident, format=format):
        id = self._ident_record('session', ident)['id']
        self._delete(f'sessions/{id}')

    def session_restart(self, ident, wait=True, open=False, frame=True, format=None):
        srec = self._ident_record('session', ident)
        id, pid = srec['id'], srec['project_id']
        self._delete(f'sessions/{id}')
        # Unlike deployments I am not copying over the editor and resource profile
        # settings from the current session. That's because I want to support the use
        # case where the session settings are patched prior to restart
        return self.session_start(pid, wait=wait, open=open, frame=frame, format=format)

    def session_open(self, ident, frame=True, format=None):
        srec = self._ident_record('session', ident)
        if frame:
            scheme, _, hostname, *_, project_id = srec['project_url'].split('/')
            url = f'{scheme}//{hostname}/projects/detail/a0-{project_id}/view'
        else:
            scheme, _, hostname, *_, session_id = srec['url'].split('/')
            url = f'{scheme}//{session_id}.{hostname}/'
        webbrowser.open(url, 1, True)

    def session_changes(self, ident, master=False, format=None):
        id = self._ident_record('session', ident)['id']
        which = 'master' if master else 'local'
        result = self._get(f'sessions/{id}/changes/{which}')
        result = self._fix_records('change', result['files'])
        return self._format_response(result, format=format)

    def session_branches(self, ident, format=None):
        id = self._ident_record('session', ident)['id']
        # Use master because it's more likely to be a smaller result (no changed files)
        result = self._get(f'sessions/{id}/changes/master')
        result = [{'branch': k, 'sha1': v} for k, v in result['branches'].items()]
        result = self._fix_records('branch', result)
        return self._format_response(result, format=format)

    def _pre_deployment(self, records):
        # Add the project ID to the deployment record
        for record in ([records] if isinstance(records, dict) else records):
            pid = 'a0-' + record['project_url'].rsplit('/', 1)[-1]
            record['project_id'] = pid
            if record.get('url'):
                record['endpoint'] = record['url'].split('/', 3)[2].split('.', 1)[0]
        return records

    def _post_deployment(self, records, collaborators=False, k8s=False):
        if collaborators:
            self._join_collaborators('deployments', records)
        if k8s:
            return self._join_k8s(records, changes=False)
        return records

    def deployment_list(self, filter=None, collaborators=False, k8s=False, format=None):
        response = self._get_records('deployments', filter=filter, collaborators=collaborators, k8s=k8s)
        return self._format_response(response, format=format)

    def deployment_info(self, ident, collaborators=False, k8s=False, format=None, quiet=False):
        record = self._ident_record('deployment', ident, collaborators=collaborators, k8s=k8s, quiet=quiet)
        return self._format_response(record, format=format)

    def _pre_endpoint(self, records):
        dlist = self.deployment_list()
        plist = self.project_list()
        dmap = {drec['endpoint']: drec for drec in dlist if drec['endpoint']}
        pmap = {prec['id']: prec for prec in plist}
        newrecs = []
        for rec in records:
            drec = dmap.get(rec['id'])
            if drec:
                rec['name'], rec['deployment_id'] = drec['name'], drec['id']
                rec['project_url'] = drec['project_url']
                rec['owner'] = drec['owner']
                rec['_deployment'] = drec
            else:
                rec['name'], rec['deployment_id'] = '', ''
            rec['project_id'] = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
            prec = pmap.get(rec['project_id'])
            if prec:
                rec['project_name'] = prec['name']
                rec.setdefault('owner', prec['owner'])
                rec['_project'] = prec
                rec['_record_type'] = 'endpoint'
                newrecs.append(rec)
        return newrecs

    def endpoint_list(self, filter=None, format=None):
        response = self._get('/platform/deploy/api/v1/apps/static-endpoints')['data']
        response = self._fix_records('endpoint', response, filter=filter)
        return self._format_response(response, format=format)

    def endpoint_info(self, ident, format=None, quiet=False):
        response = self._ident_record('endpoint', ident)
        return self._format_response(response, format=format)

    def deployment_collaborator_list(self, ident, filter=None, format=None):
        id = self._ident_record('deployment', ident)['id']
        response = self._get_records(f'deployments/{id}/collaborators', filter)
        return self._format_response(response, format=format)

    def deployment_collaborator_info(self, ident, userid, format=None, quiet=False):
        filter = f'id={userid}'
        response = self.deployment_collaborator_list(ident, filter=filter)
        response = self._should_be_one(response, filter, quiet)
        return self._format_response(response, format=format)

    def deployment_collaborator_list_set(self, ident, collabs, format=None):
        id = self._ident_record('deployment', ident)['id']
        result = self._put(f'deployments/{id}/collaborators', json=collabs)
        if result['action']['error'] or 'collaborators' not in result:
            raise AEException(f'Unexpected error adding collaborator: {result}')
        result = self._fix_records('collaborator', result['collaborators'])
        return self._format_response(result, format=format)

    def deployment_collaborator_add(self, ident, userid, group=False, format=None):
        drec = self._ident_record('deployment', ident)
        collabs = self.deployment_collaborator_list(drec)
        ncollabs = len(collabs)
        if not isinstance(userid, tuple):
            userid = userid,
        collabs = [c for c in collabs if c['id'] not in userid]
        if len(collabs) != ncollabs:
            self.deployment_collaborator_list_set(drec, collabs)
        collabs.extend({'id': u, 'type': 'group' if group else 'user', 'permission': 'r'} for u in userid)
        return self.deployment_collaborator_list_set(drec, collabs, format=format)

    def deployment_collaborator_remove(self, ident, userid, format=None):
        drec = self._ident_record('deployment', ident)
        collabs = self.deployment_collaborator_list(drec)
        if not isinstance(userid, tuple):
            userid = userid,
        missing = set(userid) - set(c['id'] for c in collabs)
        if missing:
            missing = ', '.join(missing)
            raise AEException(f'Collaborator(s) not found: {missing}')
        collabs = [c for c in collabs if c['id'] not in userid]
        return self.deployment_collaborator_list_set(drec, collabs, format=format)

    def deployment_start(self, ident, name=None, endpoint=None, command=None,
                         resource_profile=None, public=False,
                         collaborators=None, wait=True, open=False, frame=False,
                         stop_on_error=False, format=None,
                         _skip_endpoint_test=False):
        rrec = self._revision(ident, keep_latest=True)
        id, prec = rrec['project_id'], rrec['_project']
        if command is None:
            command = rrec['commands'].split(',', 1)[0]
        if resource_profile is None:
            resource_profile = prec['resource_profile']
        data = {'source': rrec['url'],
                'revision': rrec['name'],
                'resource_profile': resource_profile,
                'command': command,
                'public': bool(public),
                'target': 'deploy'}
        if name:
            data['name'] = name
        if endpoint:
            if not re.match(r'[A-Za-z0-9-]+', endpoint):
                raise AEException(f'Invalid endpoint: {endpoint}')
            if not _skip_endpoint_test:
                try:
                    self._head(f'/_errors/404.html', subdomain=endpoint)
                    raise AEException('endpoint "{}" is already in use'.format(endpoint))
                except AEUnexpectedResponseError:
                    pass
            data['static_endpoint'] = endpoint
        response = self._post_record(f'projects/{id}/deployments', api_kwargs={'json': data})
        id = response['id']
        if response.get('error'):
            raise AEException('Error starting deployment: {}'.format(response['error']['message']))
        if collaborators:
            self.deployment_collaborator_list_set(id, collaborators)
        # The _wait method doesn't work here. The action isn't even updated, it seems
        if wait or stop_on_error:
            while response['state'] in ('initial', 'starting'):
                time.sleep(2)
                response = self._get_records(f'deployments/{id}', record_type='deployment')
            if response['state'] != 'started':
                if stop_on_error:
                    self.deployment_stop(id)
                raise AEException(f'Error completing deployment start: {response["status_text"]}')
        if open:
            self.deployment_open(response, frame)
        return self._format_response(response, format=format)

    def deployment_restart(self, ident, wait=True, open=False, frame=True, stop_on_error=False, format=None):
        drec = self._ident_record('deployment', ident)
        collab = self.deployment_collaborator_list(drec)
        if drec.get('url'):
            endpoint = drec['url'].split('/', 3)[2].split('.', 1)[0]
            if drec['id'].endswith(endpoint):
                endpoint = None
        else:
            endpoint = None
        self.deployment_stop(drec)
        return self.deployment_start('{}:{}'.format(drec['project_id'], drec['revision']),
                                     endpoint=endpoint, command=drec['command'],
                                     resource_profile=drec['resource_profile'],
                                     public=drec['public'],
                                     collaborators=collab, wait=wait, open=open, frame=frame,
                                     stop_on_error=stop_on_error, format=format,
                                     _skip_endpoint_test=True)

    def deployment_open(self, ident, frame=False, format=None):
        drec = self._ident_record('deployment', ident)
        scheme, _, hostname, _ = drec['project_url'].split('/', 3)
        if frame:
            url = f'{scheme}//{hostname}/deployments/detail/{drec["id"]}/view'
        else:
            url = drec['url']
        webbrowser.open(url, 1, True)

    def deployment_patch(self, ident, format=None, **kwargs):
        drec = self._ident_record('deployment', ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            id = drec['id']
            self._patch(f'deployments/{id}', json=data)
            drec = self._ident_record('deployment', id)
        return self._format_response(drec, format=format)

    def deployment_stop(self, ident, format=None):
        id = self._ident_record('deployment', ident)['id']
        self._delete(f'deployments/{id}')

    def deployment_logs(self, ident, which=None, format=None):
        id = self._ident_record('deployment', ident)['id']
        response = self._get(f'deployments/{id}/logs')
        if which is not None:
            response = response[which]
        return self._format_response(response, format=format)

    def deployment_token(self, ident, which=None, format=None):
        id = self._ident_record('deployment', ident)['id']
        response = self._post(f'deployments/{id}/token', format='json')
        if isinstance(response, dict) and set(response) == {'token'}:
            response = response['token']
        return self._format_response(response, format=format)

    def _pre_job(self, records):
        precs = {x['id']: x for x in self._get_records('projects')}
        for rec in records:
            if rec.get('project_url'):
                pid = 'a0-' + (rec.get('project_url') or '').rsplit('/', 1)[-1]
                prec = precs.get(pid, {})
                rec['project_id'] = pid
                rec['_project'] = prec
                rec
        return records

    def job_list(self, filter=None, format=None):
        response = self._get_records('jobs', filter=filter)
        return self._format_response(response, format=format)

    def job_info(self, ident, format=None, quiet=False):
        response = self._ident_record('job', ident, quiet=quiet)
        return self._format_response(response, format=format)

    def job_runs(self, ident, format=None):
        id = self._ident_record('job', ident)['id']
        response = self._get_records(f'jobs/{id}/runs')
        return self._format_response(response, format=format)

    def job_delete(self, ident, format=None):
        id = self._ident_record('job', ident)['id']
        self._delete(f'jobs/{id}')

    def job_pause(self, ident, format=None):
        id = self._ident_record('job', ident)['id']
        response = self._post_record(f'jobs/{id}/pause', record_type='job')
        return self._format_response(response, format=format)

    def job_unpause(self, ident, format=format):
        id = self._ident_record('job', ident)['id']
        response = self._post_record(f'jobs/{id}/unpause', record_type='job')
        return self._format_response(response, format=format)

    def job_create(self, ident, schedule=None, name=None, command=None,
                   resource_profile=None, variables=None, run=None,
                   wait=None, cleanup=False, make_unique=None,
                   show_run=False, format=None):
        if run is None:
            run = not schedule or cleanup
        if wait is None:
            wait = cleanup
        if cleanup and schedule:
            raise ValueError('cannot use cleanup=True with a scheduled job')
        if cleanup and (not run or not wait):
            raise ValueError('must specify run=wait=True with cleanup=True')
        rrec = self._revision(ident, keep_latest=True)
        prec, id = rrec['_project'], rrec['project_id']
        if not command:
            command = rrec['commands'][0]['id']
        if not resource_profile:
            resource_profile = rrec['_project']['resource_profile']
        # AE5's default name generator unfortunately uses colons
        # in the creation of its job names which causes confusion for
        # ae5-tools, which uses them to mark a revision identifier.
        # Furthermore, creating a job with the same name as an deleted
        # job that still has run listings causes an error.
        if not name:
            name = f'{command}-{prec["name"]}'
            if make_unique is None:
                make_unique = True
        if make_unique:
            jnames = {j['name'] for j in self._get(f'jobs')}
            jnames.update(j['name'] for j in self._get(f'runs'))
            if name in jnames:
                bname = name
                for counter in range(1, len(jnames) + 1):
                    name = f'{bname}-{counter}'
                    if name not in jnames:
                        break
        data = {'source': rrec['url'],
                'resource_profile': resource_profile,
                'command': command,
                'target': 'deploy',
                'schedule': schedule,
                'autorun': run,
                'revision': rrec['name'],
                'name': name}
        if variables:
            data['variables'] = variables
        response = self._post_record(f'projects/{id}/jobs', api_kwargs={'json': data})
        if response.get('error'):
            raise AEException('Error starting job: {}'.format(response['error']['message']))
        if run:
            jid = response['id']
            run = self._get_records(f'jobs/{jid}/runs')[-1]
            if wait:
                rid = run['id']
                while run['state'] not in ('completed', 'error'):
                    time.sleep(5)
                    run = self._get(f'runs/{rid}')
                if cleanup:
                    self._delete(f'jobs/{jid}')
            if show_run:
                response = run
        return self._format_response(response, format=format)

    def job_patch(self, ident, name=None, command=None, schedule=None,
                  resource_profile=None, variables=None, format=None):
        jrec = self._ident_record('job', ident)
        id = jrec['id']
        data = {}
        if name and name != jrec['name']:
            data['name'] = name
        if command and command != jrec['command']:
            data['command'] = command
        if schedule and schedule != jrec['schedule']:
            data['schedule'] = schedule
        if resource_profile and resource_profile != jrec['resource_profile']:
            data['resource_profile'] = resource_profile
        if variables is not None and data['variables'] != jrec['variables']:
            data['variables'] = variables
        if data:
            self._patch_record(f'jobs/{id}', json=data)
            jrec = self._ident_record('job', id)
        return self._format_response(jrec, format=format)

    # runs need the same preprocessing as jobs,
    # and the same postprocessing as sessions
    _pre_run = _pre_job
    _post_run = _post_session

    def run_list(self, k8s=False, filter=None, format=None):
        response = self._get_records('runs', k8s=k8s, filter=filter)
        return self._format_response(response, format=format)

    def run_info(self, ident, k8s=False, format=None, quiet=False):
        response = self._ident_record('run', ident, k8s=k8s, quiet=quiet)
        return self._format_response(response, format=format)

    def run_log(self, ident, format=None):
        id = self._ident_record('run', ident)['id']
        response = self._get(f'runs/{id}/logs')['job']
        return response

    def run_stop(self, ident, format=None):
        id = self._ident_record('run', ident)['id']
        response = self._post(f'runs/{id}/stop')
        return self._format_response(response, format=format)

    def run_delete(self, ident, format=None):
        id = self._ident_record('run', ident)['id']
        self._delete(f'runs/{id}')

    def _pre_pod(self, records):
        result = []
        for rec in records:
            if 'project_id' in rec:
                type = rec['_record_type']
                value = {k: rec[k] for k in ('name', 'owner', 'resource_profile', 'id', 'project_id')}
                value['type'] = type
                result.append(rec)
        return result

    def _post_pod(self, records):
        return self._join_k8s(records, changes=True)

    def pod_list(self, filter=None, format=None):
        records = (self.session_list(filter=filter) +
                   self.deployment_list(filter=filter) +
                   self.run_list(filter=filter))
        records = self._fix_records('pod', records)
        return self._format_response(records, format=format)

    def pod_info(self, pod, format=None, quiet=False):
        record = self._ident_record('pod', pod, quiet=quiet)
        return self._format_response(record, format=format)

    def node_list(self, filter=None, format=None):
        result = []
        for rec in self._k8s('node_info'):
            result.append({
                'name': rec['name'],
                'role': rec['role'],
                'ready': rec['ready'],
                'capacity/pod': rec['capacity']['pods'],
                'capacity/mem': rec['capacity']['mem'],
                'capacity/cpu': rec['capacity']['cpu'],
                'capacity/gpu': rec['capacity']['gpu'],
                'usage/pod': rec['total']['pods'],
                'usage/mem': rec['total']['usage']['mem'],
                'usage/cpu': rec['total']['usage']['cpu'],
                'usage/gpu': rec['total']['usage']['gpu'],
                'sessions/pod': rec['sessions']['pods'],
                'sessions/mem': rec['sessions']['usage']['mem'],
                'sessions/cpu': rec['sessions']['usage']['cpu'],
                'sessions/gpu': rec['sessions']['usage']['gpu'],
                'deployments/pod': rec['deployments']['pods'],
                'deployments/mem': rec['deployments']['usage']['mem'],
                'deployments/cpu': rec['deployments']['usage']['cpu'],
                'deployments/gpu': rec['deployments']['usage']['gpu'],
                'middleware/pod': rec['middleware']['pods'],
                'middleware/mem': rec['middleware']['usage']['mem'],
                'middleware/cpu': rec['middleware']['usage']['cpu'],
                'system/pod': rec['system']['pods'],
                'system/mem': rec['system']['usage']['mem'],
                'system/cpu': rec['system']['usage']['cpu'],
                '_k8s': rec,
                '_record_type': 'node'
            })
        result = self._fix_records('node', result, filter)
        return self._format_response(result, format=format)

    def node_info(self, node, format=None, quiet=False):
        record = self._ident_record('node', node, quiet=quiet)
        return self._format_response(record, format=format)

    def _get_v1_token(self):
        if isinstance(self, AEAdminSession):
            # TODO: impersonate
            raise NotImplementedError('We do not have impersonation working to get the authorization token.')
        else:
            url = f'https://{self.hostname}/auth/realms/AnacondaPlatform/protocol/openid-connect/token'
            v1_filename = os.path.join(config._path, 'v1-tokens', f'{self.username}@{self.hostname}')
            os.makedirs(os.path.dirname(v1_filename), mode=0o700, exist_ok=True)

            if os.path.exists(v1_filename):
                with open(v1_filename, 'r') as fp:
                    current_sdata = json.load(fp)
                if isinstance(current_sdata, dict) and 'refresh_token' in current_sdata:
                    resp = self.session.post(url,
                                             data={'refresh_token': current_sdata['refresh_token'],
                                                   'grant_type': 'refresh_token',
                                                   'scope': 'offline_access',
                                                   'client_id': 'anaconda-platform',
                                                   'client_secret': 'ed7ec3ff-c535-455b-b431-5ed97d78b8be'
                                                   })
                    if resp.status_code == 200:
                        sdata = resp.json()
                        with open(v1_filename, 'w') as f:
                            json.dump(sdata, f)
            else:
                # borrowed from .authorize()
                key = f'{self.username}@{self.hostname}'
                need_password = self.password is None
                last_valid = True
                if need_password:
                    password = self._password_prompt(key, last_valid)
                else:
                    password = self.password
                if not need_password:
                    raise AEException('Invalid username or password.')

                data = {
                    'username': self.username,
                    'password': password,
                    'grant_type': 'password',
                    'scope': 'offline_access',
                    'client_id': 'anaconda-platform',
                    'client_secret': 'ed7ec3ff-c535-455b-b431-5ed97d78b8be'
                }

                r = self.session.post(url, data=data)
                r.raise_for_status()
                sdata = r.json()
                with open(v1_filename, 'w') as f:
                    json.dump(sdata, f)

            return sdata['access_token']

    def git_config(self, *git_config_flags, **git_config_kwargs):
        token = self._get_v1_token()
        extraheader = f'AUTHORIZATION: bearer {token}'

        args = '--local'
        # subprocess.check_call(f'git config {args} http.https://{self.hostname}.extraheader "{extraheader}"',
        #                       shell=True)
        subprocess.check_call(f'git config {args} http.extraheader "{extraheader}"',
                              shell=True)

    def post_revision_metadata(self, tags=None, verbose=True, dry_run=False, format=None):
        # Determine the tag to POST
        if tags is None:
            # find the most recent tag
            all_tags = subprocess.check_output("git tag --sort=creatordate",
                                               shell=True).decode().splitlines()
        else:
            all_tags = tags

        if verbose:
            print(f'-- All known tags: {all_tags}')

        project_id = subprocess.check_output('git config remote.origin.project', shell=True).decode().strip()
        if not project_id:
            raise RuntimeError('un able to determine project id.')

        revisions = self.revision_list(project_id)

        # To avoid conflicts later get the previously.
        # Post tags (either from UI or this script).
        posted_tags = [v['id'] for v in revisions]
        remaining_tags = [t for t in all_tags if t not in posted_tags]

        if verbose:
            print(f"""-- Known version tags
{posted_tags}
""")
            print(f"""-- Version tags to post
{remaining_tags}
""")

        # If the tag already posted ignore exit
        # since there may be new un-tagged commits
        # in this git push.
        for tag in remaining_tags:
            with TemporaryDirectory() as tempdir:
                try:
                    project_file = subprocess.check_output(f'git --no-pager show {tag}:anaconda-project.yml',
                                                           shell=True).decode()
                    with open(os.path.join(tempdir, 'anaconda-project.yml'), 'wt') as f:
                        f.write(project_file)
                    project = Project('.')
                    pubinfo = project.publication_info()
                except Exception as exc:
                    if verbose:
                        print('-- Corrupt project metadata for tag {}; skipping'.format(tag))
                        print('-- Exception: {}'.format(exc))
                    pubinfo = {
                        'commands': {
                            'ERROR': {
                                'description': 'This version of anaconda-project.yml is corrupt. Please fix and push a new commit.',
                                'default': True
                            }
                        }
                    }
                    
                body = {'data':{'type':'version','attributes':{'name':tag,'metadata':pubinfo}}}

                if verbose:
                    print('-- The metadata to be posted:')
                    pprint(body)

                if not dry_run:
                    project_url = self.project_info(project_id)['url']
                    if 'anaconda-enterprise-ap-storage' in project_url:
                        _project_url = project_url.replace('http://anaconda-enterprise-ap-storage',
                                                           f'https://{self.hostname}/platform/storage/api/v1')
                        versions_url = os.path.join(_project_url, 'versions')
                    else:
                        versions_url = os.path.join(project_url, 'versions')
                        
                    token = self._get_v1_token()
                    headers = {
                        'Authorization': f'Bearer {token}',
                        'Content-Type': 'application/vnd.api+json'
                    }

                    res = self.session.post(versions_url, headers=headers, data=json.dumps(body))
                    if verbose:
                        print(f"""-- POST request returned
{res}
{res.reason}
""")
                    res.raise_for_status()


class AEAdminSession(AESessionBase):
    def __init__(self, hostname, username, password=None, persist=True):
        self._sdata = None
        self._login_base = f'https://{hostname}/auth/realms/master/protocol/openid-connect'
        super(AEAdminSession, self).__init__(hostname, username, password,
                                             prefix='auth/admin/realms/AnacondaPlatform',
                                             persist=persist)

    def _load(self):
        self._filename = os.path.join(config._path, 'tokens', f'{self.username}@{self.hostname}')
        if os.path.exists(self._filename):
            with open(self._filename, 'r') as fp:
                sdata = json.load(fp)
            if isinstance(sdata, dict) and 'refresh_token' in sdata:
                resp = self.session.post(self._login_base + '/token',
                                         data={'refresh_token': sdata['refresh_token'],
                                               'grant_type': 'refresh_token',
                                               'client_id': 'admin-cli'})
                if resp.status_code == 200:
                    self._sdata = resp.json()

    def _connected(self):
        return isinstance(self._sdata, dict) and 'access_token' in self._sdata

    def _set_header(self):
        self.session.headers['Authorization'] = f'Bearer {self._sdata["access_token"]}'

    def _connect(self, password):
        resp = self.session.post(self._login_base + '/token',
                                 data={'username': self.username,
                                       'password': password,
                                       'grant_type': 'password',
                                       'client_id': 'admin-cli'})
        self._sdata = {} if resp.status_code == 401 else resp.json()

    def _disconnect(self):
        if self._sdata:
            self.session.post(self._login_base + '/logout',
                              data={'refresh_token': self._sdata['refresh_token'],
                                    'client_id': 'admin-cli'})
            self._sdata.clear()

    def _save(self):
        os.makedirs(os.path.dirname(self._filename), mode=0o700, exist_ok=True)
        with open(self._filename, 'w') as fp:
            json.dump(self._sdata, fp)

    def _get_paginated(self, path, **kwargs):
        records = []
        limit = kwargs.pop('limit', sys.maxsize)
        kwargs.setdefault('first', 0)
        while True:
            kwargs['max'] = min(KEYCLOAK_PAGE_MAX, limit)
            t_records = self._get(path, params=kwargs)
            records.extend(t_records)
            n_records = len(t_records)
            if n_records < kwargs['max'] or n_records == limit:
                return records
            kwargs['first'] += n_records
            limit -= n_records

    def user_events(self, format=None, **kwargs):
        first = kwargs.pop('first', 0)
        limit = kwargs.pop('limit', sys.maxsize)
        records = self._get_paginated('events', limit=limit, first=first, **kwargs)
        return self._format_response(records, format=format, columns=[])

    def _post_user(self, users):
        users = {u['id']: u for u in users}
        events = self._get_paginated('events', client='anaconda-platform', type='LOGIN')
        for e in events:
            if 'response_mode' not in e['details']:
                urec = users.get(e['userId'])
                if urec and 'lastLogin' not in urec:
                    urec['lastLogin'] = e['time']
        users = list(users.values())
        for urec in users:
            urec.setdefault('lastLogin', 0)
        return users

    def user_list(self, filter=None, format=None):
        users = self._get_paginated('users')
        users = self._fix_records('user', users, filter)
        return self._format_response(users, format=format)

    def user_info(self, ident, format=None, quiet=False):
        response = self._ident_record('user', ident, quiet=False)
        return self._format_response(response, format)

    def impersonate(self, user_or_id):
        record = self.user_info(user_or_id)
        old_headers = self.session.headers.copy()
        try:
            self._post(f'users/{record["id"]}/impersonation')
            params = {'client_id': 'anaconda-platform',
                      'scope': 'openid',
                      'response_type': 'code',
                      'redirect_uri': f'https://{self.hostname}/login'}
            self._get('/auth/realms/AnacondaPlatform/protocol/openid-connect/auth', params=params)
            cookies, self.session.cookies = self.session.cookies, LWPCookieJar()
            return cookies
        finally:
            self.session.cookies.clear()
            self.session.headers = old_headers
