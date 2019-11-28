import requests
import time
import io
import re
import os
import sys
import json
from os.path import basename, abspath, isfile, isdir, join
from fnmatch import fnmatch
from datetime import datetime
from dateutil import parser
import getpass
from tempfile import TemporaryDirectory
import tarfile

from .config import config
from .identifier import Identifier
from .docker import get_dockerfile, get_condarc
from .docker import build_image
from .archiver import create_tar_archive
from .k8s.client import AE5K8SLocalClient, AE5K8SRemoteClient

from http.cookiejar import LWPCookieJar
from requests.packages import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Maximum page size in keycloak
KEYCLOAK_PAGE_MAX = os.environ.get('KEYCLOAK_PAGE_MAX', 1000)

# Default subdomain for kubectl service
DEFAULT_K8S_ENDPOINT = 'k8s'

COLUMNS = {
    'project': ['name', 'owner', 'collaborators', 'editor', 'resource_profile', 'id', 'created', 'updated', 'project_create_status', 's3_bucket', 's3_path', 'git_server', 'repository', 'repo_owned', 'git_repos', 'repo_url', 'url'],
    'revision': ['name', 'latest', 'owner', 'commands', 'created', 'id', 'project_id', 'project_name', 'updated', 'url'],
    'command': ['id', 'supports_http_options', 'unix', 'windows', 'env_spec'],
    'collaborator': ['id', 'permission', 'type', 'first_name', 'last_name', 'email'],
    'session': ['name', 'owner', 'usage/mem', 'usage/cpu', 'usage/gpu', 'node', 'rst', 'resource_profile', 'id', 'created', 'updated', 'state', 'project_id', 'session_name', 'project_branch', 'iframe_hosts', 'url', 'project_url'],
    'resource_profile': ['name', 'description', 'cpu', 'memory', 'gpu'],
    'editor': ['id', 'packages', 'name', 'is_default'],
    'sample': ['name', 'id', 'is_template', 'is_default', 'description', 'download_url', 'owner', 'created', 'updated'],
    'deployment': ['endpoint', 'name', 'owner', 'usage/mem', 'usage/cpu', 'usage/gpu', 'node', 'rst', 'public', 'collaborators', 'command', 'revision', 'resource_profile', 'id', 'created', 'updated', 'state', 'project_id', 'project_name', 'project_owner'],
    'job': ['name', 'owner', 'command', 'revision', 'resource_profile', 'id', 'created', 'updated', 'state', 'project_id', 'project_name'],
    'run': ['name', 'owner', 'command', 'revision', 'resource_profile', 'id', 'created', 'updated', 'state', 'project_id', 'project_name'],
    'branch': ['branch', 'sha1'],
    'change': ['path', 'change_type', 'modified', 'conflicted', 'id'],
    'user': ['username', 'firstName', 'lastName', 'lastLogin', 'email', 'id'],
    'activity': ['type', 'status', 'message', 'done', 'owner', 'id', 'description', 'created', 'updated'],
    'endpoint': ['id', 'owner', 'name', 'project_name', 'deployment_id', 'project_id', 'project_url'],
    'pod': ['name', 'owner', 'type', 'usage/mem', 'usage/cpu', 'usage/gpu', 'node', 'rst', 'modified', 'phase', 'since', 'resource_profile', 'id'],
}

_DTYPES = {'created': 'datetime', 'updated': 'datetime',
           'since': 'datetime', 'mtime': 'datetime', 'timestamp': 'datetime',
           'createdTimestamp': 'timestamp/ms', 'notBefore': 'timestamp/s',
           'lastLogin': 'timestamp/ms', 'time': 'timestamp/ms'}


class EmptyRecordList(list):
    def __init__(self, rtype):
        self._record_type = rtype
        super(EmptyRecordList, self).__init__()


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
        if (sys.meta_path is not None and hasattr(self, 'persist') and
            not self.persist and self.connected):
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

    def _format_table(self, response, columns):
        is_series = isinstance(response, dict)
        if is_series:
            response = [response]
        clist = list(columns or ())
        if not response:
            return response, clist
        csrc = set(clist)
        cdst = set()
        cdashed = []
        has_rtype = False
        for rec in response:
            for c in rec:
                cdst.add(c)
                if c not in csrc:
                    csrc.add(c)
                    if c.startswith('_'):
                        if c == '_record_type':
                            has_rtype = True
                        else:
                            cdashed.append(c)
                    else:
                        clist.append(c)
        clist.extend(cdashed)
        clist = [c for c in clist if c in cdst]
        if has_rtype:
            clist.append('_record_type')
        for col, dtype in _DTYPES.items():
            if col in csrc:
                if dtype == 'datetime':
                    for rec in response:
                        if rec.get(col):
                            try:
                                rec[col] = parser.isoparse(rec[col])
                            except ValueError:
                                pass
                elif dtype.startswith('timestamp'):
                    incr = dtype.rsplit('/', 1)[1]
                    fact = 1000.0 if incr == 'ms' else 1.0
                    for rec in response:
                        if rec.get(col):
                            rec[col] = datetime.fromtimestamp(rec[col] / fact)
        if is_series:
            result = [(k, response[0].get(k)) for k in clist]
            clist = ['field', 'value']
        else:
            result = [tuple(rec.get(k) for k in clist) for rec in response]
        return (result, clist)

    def _format_response(self, response, format, columns=None, record_type=None):
        if not isinstance(response, (list, dict)):
            if response is not None and format == 'table':
                raise AEException('Response is not a tabular format')
            return response
        elif format == 'json':
            return response
        if record_type is not None:
            for rec in ([response] if isinstance(response, dict) else response):
                rec['_record_type'] = record_type
        if format not in ('table', 'tableif', 'dataframe'):
            return response
        if columns is None:
            if record_type is None:
                if response:
                    rec0 = response[0] if isinstance(response, list) else response
                    record_type = rec0.get('_record_type', '')
                elif hasattr(response, '_record_type'):
                    record_type = response._record_type
            columns = COLUMNS.get(record_type, ())
        records, columns = self._format_table(response, columns)
        if format == 'dataframe':
            try:
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
        ctype = response.headers['content-type']
        if 'json' in ctype:
            return response.json()
        elif format in ('json', 'table'):
            raise AEException(f'Content type {ctype} not compatible with json format')
        elif 'text' in ctype:
            return response.text
        else:
            return response.content

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
        self._k8s_endpoint = k8s_endpoint or 'k8s'
        self._k8s_client = None

    def _k8s(self, method, *args, **kwargs):
        quiet = kwargs.pop('quiet', False)
        if self._k8s_endpoint:
            if self._k8s_endpoint.startswith('ssh:'):
                self._k8s_client = AE5K8SLocalClient(hostname, self._k8s_endpoint.split(':', 1)[1])
            else:
                try:
                    response = self._head(f'/_errors/404.html', subdomain=self._k8s_endpoint, format='response')
                    self._k8s_client = AE5K8SRemoteClient(self, self._k8s_endpoint)
                except AEUnexpectedResponseError:
                    self._k8s_endpoint = None
                    raise AEException('No kubectl deployment was found')
            if self._k8s_endpoint and not self._k8s_client.healthy():
                self._k8s_endpoint = self._k8s_client = None
                raise AEException('Error establishing kubectl connection')
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

    def _fix_records(self, records, filter, record_type):
        if isinstance(records, dict) and 'data' in records:
            records = records['data']
        is_single = isinstance(records, dict)
        if is_single:
            records = [records]
        if records:
            fixer = f'_fix_{record_type}s'
            if hasattr(self, fixer):
                getattr(self, fixer)(records)
            if filter:
                filter = self._id_filter(filter, record_type)
                records = [rec for rec in records
                           if all(fnmatch(rec.get(k), v)
                           for k, v in filter.items())]
            for rec in records:
                rec['_record_type'] = record_type
        if is_single:
            return records[0] if records else None
        else:
            return records if records else EmptyRecordList(record_type)

    def _get_records(self, endpoint, filter=None, **kwargs):
        record_type = kwargs.pop('record_type', None)
        if not record_type:
            record_type = endpoint.rsplit('/', 1)[-1].rstrip('s')
        records = self._get(endpoint, **kwargs)
        records = self._fix_records(records, filter, record_type)
        return records

    def _post_record(self, endpoint, **kwargs):
        record_type = kwargs.pop('record_type', None)
        if not record_type:
            record_type = endpoint.rsplit('/', 1)[-1].rstrip('s')
        records = self._post(endpoint, **kwargs)
        records = self._fix_records(records, None, record_type)
        return records

    def _put_record(self, endpoint, **kwargs):
        record_type = kwargs.pop('record_type', None)
        if not record_type:
            record_type = endpoint.rsplit('/', 1)[-1].rstrip('s')
        records = self._put(endpoint, **kwargs)
        records = self._fix_records(records, None, record_type)
        return records

    def _id_filter(self, ident, type, drop_revision=True):
        if not type.endswith('s'):
            type += 's'
        if not ident:
            return {}
        if isinstance(ident, dict):
            return ident
        if isinstance(ident, str):
            ident = Identifier.from_string(ident, no_revision=type != 'projects')
        filter = ident.to_dict(drop_revision=drop_revision)
        if 'pid' in filter:
            if type != 'projects':
                filter['project_id'] = filter['id']
                if filter.get('id') == filter['pid']:
                    del filter['id']
            del filter['pid']
        if 'id' in filter:
            idtype = ident.id_type(filter['id'])
            tval = 'deployments' if type in ('jobs', 'runs') else type
            if type != 'pods' and idtype != tval:
                raise ValueError(f'Expected a {type} ID type, found a {idtype} ID: {ident}')
        return filter

    def _should_be_one(self, matches, type, ident, quiet):
        if isinstance(matches, dict):
            return matches
        if len(matches) == 1:
            return matches[0]
        if quiet:
            return None
        pfx = 'Multiple' if len(matches) else 'No'
        if isinstance(ident, str):
            ident = Identifier.from_string(ident).project_filter() or '*'
        if isinstance(ident, dict):
            ident = ','.join(f'{k}={v}' for k, v in ident.items())
        msg = f'{pfx} {type} found matching {ident}'
        if matches:
            if type == 'revisions':
                matches = [r['name'] for r in matches]
            else:
                matches = [str(Identifier.from_record(r, True)) for r in matches]
            msg += ':\n  - ' + '\n  - '.join(matches)
        raise AEException(msg)

    def _id(self, type, ident, quiet=False):
        not_native = type.startswith('@')
        if not_native:
            type = type[1:]
        rtype = type[:-1]
        if isinstance(ident, dict) and ident.get('_record_type', '') == rtype:
            return ident
        filter = self._id_filter(ident, type)
        if not_native:
            matches = getattr(self, f'{rtype}_list')(filter=filter)
        else:
            if 'id' in filter:
                url = f'{type}/{filter["id"]}'
                del filter['id']
            else:
                url = type
            matches = self._get_records(url, filter, record_type=rtype)
        return self._should_be_one(matches, type, ident, quiet)

    def _id_or_name(self, type, ident, quiet=False):
        matches = []
        records = getattr(self, type.rstrip('s') + '_list')()
        has_id = any('id' in rec for rec in records)
        for rec in records:
            if (has_id and fnmatch(rec['id'], ident) or fnmatch(rec['name'], ident)):
                matches.append(rec)
        if len(matches) > 1 and has_id:
            attempt = [rec for rec in matches if fnmatch(rec['id'], ident)]
            if len(attempt) == 1:
                matches = attempt
        if len(matches) == 1:
            return matches[0]
        elif quiet:
            return None
        else:
            tstr = type.replace('_', ' ')
            pfx = 'Multiple' if len(matches) else 'No'
            msg = f'{pfx} {tstr}s found matching "{ident}"'
            if matches:
                if has_id:
                    matches = [f'{r["id"]}: {r["name"]}' for r in matches]
                else:
                    matches = [r["name"] for r in matches]
                msg += ':\n  - ' + '\n  - '.join(matches)
            raise AEException(msg)

    def project_list(self, filter=None, internal=False, collaborators=False, format=None):
        records = self._get_records('projects', filter=filter)
        if not internal and collaborators:
            self._join_collaborators('projects', records)
        return self._format_response(records, format=format)

    def project_info(self, ident, internal=False, collaborators=False, format=None, quiet=False):
        # We're hitting the list endpoint instead of the single-record endpoint because
        # for some reason the individual records don't return project_create_status
        record = self.project_list(filter=ident)
        record = self._should_be_one(record, 'projects', ident, quiet)
        if record and not internal and collaborators:
            self._join_collaborators('projects', record)
        return self._format_response(record, format=format)

    def project_patch(self, ident, **kwargs):
        format = kwargs.pop('format', None)
        prec = self._id('projects', ident)
        id = prec["id"]
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            self._patch(f'projects/{id}', json=data)
            prec = self._id('projects', id)
        return self._format_response(prec, format=format)

    def project_delete(self, ident, format=None):
        id = self._id('projects', ident)['id']
        self._delete(f'projects/{id}')

    def project_collaborator_list(self, ident, format=None):
        id = self._id('projects', ident)['id']
        response = self._get_records(f'projects/{id}/collaborators')
        return self._format_response(response, format=format)

    def project_collaborator_info(self, ident, userid, quiet=False, format=None):
        for c in self.project_collaborator_list(ident):
            if userid == c['id']:
                return self._format_response(c, format=format)
        if not quiet:
            raise AEException(f'Collaborator not found: {userid}')

    def project_collaborator_list_set(self, ident, collabs, format=None):
        id = self._id('projects', ident)["id"]
        result = self._put(f'projects/{id}/collaborators', json=collabs)
        if result['action']['error'] or 'collaborators' not in result:
            raise AEException(f'Unexpected error adding collaborator: {result}')
        return self._format_response(result['collaborators'], format=format, record_type='collaborator')

    def project_collaborator_add(self, ident, userid, group=False, read_only=False, format=None):
        prec = self._id('projects', ident)
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
        prec = self._id('projects', ident)
        collabs = self.project_collaborator_list(prec)
        if not isinstance(userid, tuple):
            userid = userid,
        missing = set(userid) - set(c['id'] for c in collabs)
        if missing:
            missing = ', '.join(missing)
            raise AEException(f'Collaborator(s) not found: {missing}')
        collabs = [c for c in collabs if c['id'] not in userid]
        return self.project_collaborator_list_set(prec, collabs, format=format)

    def resource_profile_list(self, format=None):
        response = self._get('projects/actions', params={'q': 'create_action'})
        profiles = response[0]['resource_profiles']
        for profile in profiles:
            profile['description'], params = profile['description'].rsplit(' (', 1)
            for param in params.rstrip(')').split(', '):
                k, v = param.split(': ', 1)
                profile[k.lower()] = v
            if 'gpu' not in profile:
                profile['gpu'] = 0
            profile['_record_type'] = 'resource_profile'
        return self._format_response(profiles, format=format)

    def resource_profile_info(self, name, format=None, quiet=False):
        record = self._id_or_name('resource_profile', name, quiet=quiet)
        return self._format_response(record, format=format)

    def editor_list(self, internal=False, format=None):
        response = self._get('projects/actions', params={'q': 'create_action'})[0]
        editors = response['editors']
        for rec in editors:
            rec['packages'] = ' '.join(rec['packages'])
            rec['_record_type'] = 'editor'
        return self._format_response(editors, format=format)

    def editor_info(self, name, format=None, quiet=False):
        record = self._id_or_name('editor', name)
        return self._format_response(record, format=format)

    def sample_list(self, format=None):
        result = []
        for response, template in ((self._get('template_projects'), True),
                                   (self._get('sample_projects'), False)):
            for record in response:
                record['is_template'] = template
                record.setdefault('is_default', False)
                record['_record_type'] = 'sample'
                result.append(record)
        return self._format_response(result, format=format)

    def sample_info(self, ident, format=None, quiet=False):
        record = self._id_or_name('sample', ident, quiet=quiet)
        return self._format_response(record, format=format)

    def sample_clone(self, ident, name=None, tag=None,
                     make_unique=None, wait=True, format=None):
        record = self._id_or_name('sample', ident)
        if name is None:
            name = record['name']
            if make_unique is None:
                make_unique = True
        return self.project_create(record['download_url'], name=name, tag=tag,
                                   make_unique=make_unique, wait=wait, format=format)

    def project_sessions(self, ident, format=None):
        id = self._id('projects', ident)["id"]
        response = self._get_records(f'projects/{id}/sessions')
        return self._format_response(response, format=format)    

    def project_deployments(self, ident, format=None):
        id = self._id('projects', ident)["id"]
        response = self._get_records(f'projects/{id}/deployments')
        return self._format_response(response, format=format)

    def project_jobs(self, ident, format=None):
        id = self._id('projects', ident)["id"]
        response = self._get_records(f'projects/{id}/jobs')
        return self._format_response(response, format=format)

    def project_runs(self, ident, format=None):
        id = self._id('projects', ident)["id"]
        response = self._get_records(f'projects/{id}/runs')
        return self._format_response(response, format=format)

    def project_activity(self, ident, limit=0, latest=False, format=None):
        id = self._id('projects', ident)["id"]
        limit = 1 if latest else (999999 if limit <= 0 else limit)
        params = {'sort': '-updated', 'page[size]': limit}
        response = self._get_records(f'projects/{id}/activity', params=params)
        if latest:
            response = response[0]
        return self._format_response(response, format=format)

    def _fix_revisions(self, revisions):
        if isinstance(revisions, dict):
            revisions = revisions[0]
        revisions[0]['latest'] = True
        for rec in revisions[1:]:
            rec['latest'] = False

    def _revisions(self, filter, latest=False, quiet=False):
        filter = self._id_filter(filter, 'projects', drop_revision=False)
        revision = filter.pop('revision', None)
        prec = self._id('projects', filter, quiet=quiet)
        if prec is None:
            return None
        id = prec["id"]
        filter = {'name': revision} if revision and revision != 'latest' else {}
        response = self._get_records(f'projects/{id}/revisions', filter)
        if revision == 'latest' or not revision and latest:
            response = [rec for rec in response if rec['latest']]
            if latest == 'keep' and response:
                response[0]['name'] == 'latest'
        elif not revision and latest:
            response = [response[0]]
        for rec in response:
            rec['project_id'] = id
            rec['project_name'] = prec['name']
            rec['_commands'] = rec['commands']
            rec['commands'] = ', '.join(c['id'] for c in rec['_commands'])
            rec['_project'] = prec
            for c in rec['_commands']:
                c['_record_type'] = 'command'
        return response

    def _revision(self, ident, keep_latest=True, quiet=False):
        latest = 'keep' if keep_latest else True
        response = self._revisions(ident, latest=latest, quiet=quiet)
        if response:
            response = self._should_be_one(response, 'revisions', ident, quiet)
        return response

    def revision_list(self, filter=None, internal=False, format=None):
        response = self._revisions(filter, quiet=False)
        return self._format_response(response, format=format)

    def revision_info(self, ident, internal=False, format=None, quiet=False):
        rrec = self._revision(ident, quiet=quiet)
        return self._format_response(rrec, format=format)

    def revision_commands(self, ident, internal=False, format=None, quiet=False):
        rrec = self._revision(ident, quiet=quiet)
        return self._format_response(rrec['_commands'], format=format)

    def project_download(self, ident, filename=None):
        rrec = self._revision(ident)
        id, rev = rrec['project_id'], rrec['id']
        response = self._get(f'projects/{id}/revisions/{rev}/archive', format='blob')
        if filename is None:
            return response
        with open(filename, 'wb') as fp:
            fp.write(response)

    def project_image(self, ident, command=None, condarc_path=None, dockerfile_path=None, debug=False, format=None):
        '''Build docker image'''
        rrec = self._revision(ident)
        prec, rev = rrec["_project"], rrec["id"]
        name = prec['name'].replace(' ','').lower()
        owner = prec['owner'].replace('@','_at_')
        tag = f'{owner}/{name}:{rev}'

        dockerfile = get_dockerfile(dockerfile_path)
        condarc = get_condarc(condarc_path)

        if command:
            commands = [c['id'] for c in rrec['commands']]
            if not commands:
                print('There are no configured commands in this project.')
                print('Remove the --command option to build the container anyway.')
                return
            if command in commands:
                dockerfile = re.sub('(CMD anaconda-project run)(.*?)$', f'\g<1> {command}', dockerfile)
            else:
                print(f'The command {command} is not one of the configured commands.')
                print('Available commands are:')
                for c in rrec['commands']:
                    default = c.get('default', False)
                    if default:
                        print(f'  {c["id"]:15s} (default)')
                    else:
                        print(f'  {c["id"]:15s}')
                return

        with TemporaryDirectory() as tempdir:
            with open(os.path.join(tempdir, 'Dockerfile'), 'w') as f:
                f.write(dockerfile)

            with open(os.path.join(tempdir, 'condarc'), 'w') as f:
                f.write(condarc)

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
        response = self._post_record('projects', json=params)
        if response.get('error'):
            raise RuntimeError('Error creating project: {}'.format(response['error']['message']))
        if wait:
            self._wait(response)
        if response['action']['error']:
            raise RuntimeError('Error processing creation: {}'.format(response['action']['message']))
        return self.project_info(response['id'], format=format)

    def project_upload(self, project_archive, name, tag, wait=True, format=None):
        if not name:
            if type(project_archive) == bytes:
                raise RuntimeError('Project name must be supplied for binary input')
            name = basename(abspath(project_archive)).split('.', 1)[0]
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
                f.seek(0)
            data = {'name': name}
            if tag:
                data['tag'] = tag
            response = self._post_record('projects/upload', files={'project_file': f}, data=data, record_type='project')
        finally:
            if f is not None:
                f.close()
        if response.get('error'):
            raise RuntimeError('Error uploading project: {}'.format(response['error']['message']))
        if wait:
            self._wait(response)
        if response['action']['error']:
            raise RuntimeError('Error processing upload: {}'.format(response['action']['message']))
        return self.project_info(response['id'], format=format)

    def _join_collaborators(self, what, response):
        if isinstance(response, dict):
            what, id = response['_record_type'], response['id']
            collabs = self._get_records(f'{what}s/{id}/collaborators')
            response['collaborators'] = ', '.join(c['id'] for c in collabs)
            response['_collaborators'] = collabs
        elif response:
            for rec in response:
                self._join_collaborators(what, rec)

    def _join_k8s(self, record, changes=False):
        is_single = isinstance(record, dict)
        if is_single:
            record = [record]
        if record:
            record2 = self._k8s('pod_info', [r['id'] for r in record])
            for rec, rec2 in zip(record, record2):
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
        nhead = ['phase', 'usage/mem', 'usage/cpu', 'usage/gpu', 'modified', 'rst', 'since', 'node']
        if not changes:
            nhead.remove('modified')
        return nhead

    def _fix_sessions(self, response):
        # The "name" value in an internal AE5 session record is nothing
        # more than the "id" value with the "a1-" stub removed. Not very
        # helpful, even if understandable. So we call _join_projects even
        # when internal=True to replace this internal name with the project
        # name, providing a more consistent user experience
        if isinstance(response, dict):
            response = response[0]
        precs = {x['id']: x for x in self._get_records('projects')}
        for rec in response:
            pid = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
            prec = precs.get(pid, {})
            rec['session_name'] = rec['name']
            rec['name'] = prec['name']
            rec['project_id'] = pid
            rec['_project'] = prec

    def session_list(self, filter=None, internal=False, k8s=False, format=None):
        records = self._get_records('sessions', filter=filter)
        if k8s:
            self._join_k8s(records, True)
        return self._format_response(records, format, record_type='session')

    def session_info(self, ident, internal=False, k8s=False, format=None, quiet=False):
        record = self._id('sessions', ident, quiet=quiet)
        if not internal and k8s:
            self._join_k8s(record, True)
        return self._format_response(record, format)

    def session_start(self, ident, editor=None, resource_profile=None, wait=True, format=None):
        prec = self._id('projects', ident)
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
        if wait:
            self._wait(response)
        if response['action'].get('error'):
            raise RuntimeError('Error completing session start: {}'.format(response['action']['message']))
        return self._format_response(response, format=format)

    def session_stop(self, ident, format=format):
        id = self._id('sessions', ident)['id']
        self._delete(f'sessions/{id}')

    def session_restart(self, ident, wait=True, format=None):
        srec = self._id('sessions', ident)
        id, pid = srec['id'], srec['project_id']
        self._delete(f'sessions/{id}')
        # Unlike deployments I am not copying over the editor and resource profile
        # settings from the current session. That's because I want to support the use
        # case where the session settings are patched prior to restart
        return self.session_start(pid, wait=wait, format=format)

    def session_changes(self, ident, master=False, format=None):
        id = self._id('sessions', ident)['id']
        which = 'master' if master else 'local'
        result = self._get_records(f'sessions/{id}/changes/{which}', record_type='change')
        return self._format_response(result['files'], format=format, record_type='change')

    def session_branches(self, ident, format=None):
        id = self._id('sessions', ident)['id']
        # Use master because it's more likely to be a smaller result (no changed files)
        result = self._get(f'sessions/{id}/changes/master')
        result = [{'branch': k, 'sha1': v, '_record_type': 'branch'} for k, v in result['branches'].items()]
        return self._format_response(result, format=format)

    def _fix_deployments(self, records):
        # Add the project ID to the deployment record
        for record in ([records] if isinstance(records, dict) else records):
            pid = 'a0-' + record['project_url'].rsplit('/', 1)[-1]
            record['project_id'] = pid
            if record.get('url'):
                record['endpoint'] = record['url'].split('/', 3)[2].split('.', 1)[0]

    def deployment_list(self, filter=None, internal=False, collaborators=False, k8s=False, format=None):
        response = self._get_records('deployments', filter=filter)
        if collaborators:
             self._join_collaborators('deployments', response)
        if k8s:
            self._join_k8s(response, False)
        return self._format_response(response, format=format)

    def deployment_info(self, ident, internal=False, collaborators=False, k8s=False, format=None, quiet=False):
        record = self._id('deployments', ident, quiet=quiet)
        if record and not internal and collaborators:
            self._join_collaborators('deployments', record)
        if record and not internal and k8s:
            self._join_k8s(record, False)
        return self._format_response(record, format=format)

    def endpoint_list(self, format=None):
        response = self._get('/platform/deploy/api/v1/apps/static-endpoints')['data']
        deps = self.deployment_list()
        dmap = {drec['endpoint']: drec for drec in deps if drec['endpoint']}
        pmap = {prec['id']: prec for prec in self._get_records('projects')}
        newrecs = []
        for rec in response:
            drec = dmap.get(rec['id'])
            if drec:
                rec['name'], rec['deployment_id'] = drec['name'], drec['id']
                rec['project_url'] = drec['project_url']
                rec['owner'] = drec['owner']
            else:
                rec['name'], rec['deployment_id'] = '', ''
            rec['project_id'] = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
            prec = pmap.get(rec['project_id'])
            if prec:
                rec['project_name'] = prec['name']
                rec.setdefault('owner', prec['owner'])
                rec['_record_type'] = 'endpoint'
                newrecs.append(rec)
        return self._format_response(newrecs, format=format)

    def endpoint_info(self, ident, format=None, quiet=False):
        record = self._id_or_name('endpoint', ident, quiet=quiet)
        return self._format_response(record, format=format)

    def deployment_collaborator_list(self, ident, format=None):
        id = self._id('deployments', ident)['id']
        response = self._get_records(f'deployments/{id}/collaborators')
        return self._format_response(response, format=format)

    def deployment_collaborator_info(self, ident, userid, format=None, quiet=False):
        collabs = self.deployment_collaborator_list(ident)
        for c in collabs:
            if userid == c['id']:
                return self._format_response(c, format=format)
        if not quiet:
            raise AEException(f'Collaborator not found: {userid}')

    def deployment_collaborator_list_set(self, ident, collabs, format=None):
        id = self._id('deployments', ident)['id']
        result = self._put(f'deployments/{id}/collaborators', json=collabs)
        return self._format_response(result['collaborators'], format=format, record_type='collaborator')

    def deployment_collaborator_add(self, ident, userid, group=False, format=None):
        drec = self._id('deployments', ident)
        collabs = self.deployment_collaborator_list(drec)
        ncollabs = len(collabs)
        if not isinstance(userid, tuple):
            userid = userid,
        collabs = [c for c in collabs if c['id'] not in userid]
        if len(collabs) != ncollabs:
            self.deployment_collaborator_list_set(id, collabs)
        collabs.extend({'id': u, 'type': 'group' if group else 'user', 'permission': 'r'} for u in userid)
        return self.deployment_collaborator_list_set(drec, collabs, format=format)

    def deployment_collaborator_remove(self, ident, userid, format=None):
        drec = self._id('deployments', ident)
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
                         collaborators=None, wait=True,
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
            if not _skip_endpoint_test:
                try:
                    self._head(f'/_errors/404.html', subdomain=endpoint)
                    raise AEException('endpoint "{}" is already in use'.format(endpoint))
                except AEUnexpectedResponseError:
                    pass
            data['static_endpoint'] = endpoint
        response = self._post_record(f'projects/{id}/deployments', json=data)
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
        return self._format_response(response, format=format)

    def deployment_restart(self, ident, wait=True, stop_on_error=False, format=None):
        drec = self._id('deployments', ident)
        collab = self.deployment_collaborator_list(drec)
        if drec.get('url'):
            endpoint = drec['url'].split('/', 3)[2].split('.', 1)[0]
            if drec['id'].endswith(endpoint):
                endpoint = None
        else:
            endpoint = None
        self.deployment_stop(drec)
        return self.deployment_start(drec['project_id'],
                                     endpoint=endpoint, command=drec['command'],
                                     resource_profile=drec['resource_profile'],
                                     public=drec['public'],
                                     collaborators=collab, wait=wait,
                                     stop_on_error=stop_on_error, format=format,
                                     _skip_endpoint_test=True)

    def deployment_patch(self, ident, format=None, **kwargs):
        drec = self._id('deployments', ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            id = drec['id']
            self._patch(f'deployments/{id}', json=data)
            drec = self._id('deployments', id)
        return self._format_response(drec, format=format)

    def deployment_stop(self, ident, format=None):
        id = self._id('deployments', ident)['id']
        self._delete(f'deployments/{id}')

    def deployment_logs(self, ident, which=None, format=None):
        id = self._id('deployments', ident)['id']
        response = self._get(f'deployments/{id}/logs')
        if which is not None:
            response = response[which]
        return self._format_response(response, format=format)

    def deployment_token(self, ident, which=None, format=None):
        id = self._id('deployments', ident)['id']
        response = self._post(f'deployments/{id}/token', format='json')
        if isinstance(response, dict) and set(response) == {'token'}:
            response = response['token']
        return self._format_response(response, format=format)

    def job_list(self, filter=None, internal=False, format=None):
        response = self._get_records('jobs', filter=filter)
        return self._format_response(response, format=format)

    def job_info(self, ident, internal=False, format=None, quiet=False):
        response = self._id('jobs', ident, quiet=quiet)
        return self._format_response(response, format=format)

    def job_runs(self, ident, format=None):
        id = self._id('jobs', ident)['id']
        response = self._get_records(f'jobs/{id}/runs')
        return self._format_response(response, format=format)

    def job_delete(self, ident, format=None):
        id = self._id('jobs', ident)['id']
        self._delete(f'jobs/{id}')

    def job_pause(self, ident, format=None):
        id = self._id('jobs', ident)['id']
        response = self._post_record(f'jobs/{id}/pause', record_type='job')
        return self._format_response(response, format=format)

    def job_unpause(self, ident, format=format):
        id = self._id('jobs', ident)['id']
        response = self._post_record(f'jobs/{id}/unpause', record_type='job')
        return self._format_response(response, format=format)

    def job_create(self, ident, schedule=None, name=None, command=None,
                   resource_profile=None, variables=None, run=False,
                   wait=False, cleanup=False, make_unique=None,
                   show_run=False, format=None):
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
            name = f'{command}-{rrec["project_name"]}'
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
        response = self._post_record(f'projects/{id}/jobs', json=data)
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
        jrec = self._id('jobs', ident)
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
            jrec = self._id('jobs', id)
        return self._format_response(jrec, format=format)

    def run_list(self, filter=None, internal=False, format=None):
        response = self._get_records('runs', filter=filter)
        return self._format_response(response, format=format)

    def run_info(self, ident, internal=False, format=None, quiet=False):
        response = self._id('runs', ident, quiet=quiet)
        return self._format_response(response, format=format)

    def run_log(self, ident, format=None):
        id = self._id('runs', ident)['id']
        response = self._get(f'runs/{id}/logs')['job']
        return response

    def run_stop(self, ident, format=None):
        id = self._id('runs', ident)['id']
        response = self._post(f'runs/{id}/stop')
        return self._format_response(response, format=format)

    def run_delete(self, ident, format=None):
        id = self._id('runs', ident)['id']
        self._delete(f'runs/{id}')

    def pod_list(self, filter=None, internal=False, format=None):
        records = []
        for type in ('session', 'deployment', 'run'):
            for rec in getattr(self, f'{type}_list')(filter=filter):
                value = {k: rec[k] for k in ('name', 'owner', 'resource_profile', 'id')}
                value['type'] = type[:4]
                value['_record_type'] = 'pod'
                records.append(value)
        if not internal:
            self._join_k8s(records, True)
        return self._format_response(records, format=format)

    def pod_info(self, pod, internal=False, format=None):
        record = self._id('@pods', pod)
        if not internal:
            self._join_k8s(record, True)
        return self._format_response(record, format=format)

    def node_list(self, filter=None, internal=False, format=None):
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
        return self._format_response(result, format=format)

    def node_info(self, node, internal=False, format=None, quiet=False):
        record = self._id_or_name('node', node, quiet=quiet)
        return self._format_response(record, format=format)


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

    def user_list(self, internal=False, format=None):
        users = self._get_paginated('users')
        if not internal:
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
        return self._format_response(users, format=format, record_type='user')

    def user_info(self, user_or_id, internal=False, format=None, quiet=False):
        if re.match(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', user_or_id):
            response = [self._get(f'users/{user_or_id}')]
        else:
            response = self._get(f'users?username={user_or_id}')
        if response:
            response = response[0]
            if not internal:
                events = self._get_paginated('events', client='anaconda-platform',
                                             type='LOGIN', user=response['id'])
                time = next((e['time'] for e in events
                             if 'response_mode' not in e['details']), 0)
                response['lastLogin'] = datetime.utcfromtimestamp(time / 1000.0)
        elif not quiet:
            raise ValueError(f'Could not find user {user_or_id}')
        if not internal:
            events = self.user_events(client='anaconda-platform', type='LOGIN', user=response['id'])
            response['lastLogin'] = next((e['time'] for e in events
                                         if 'response_mode' not in e['details']), 0)
        return self._format_response(response, format, record_type='user')

    def impersonate(self, user_or_id):
        record = self.user_info(user_or_id, internal=True)
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
