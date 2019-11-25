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


_P_COLUMNS  = [            'name', 'owner', 'editor',   'resource_profile',                           'id', 'created', 'updated',                        'url']  # noqa: E241, E201
_S_COLUMNS  = [            'name', 'owner',             'resource_profile',                           'id', 'created', 'updated', 'state', 'project_id', 'url']  # noqa: E241, E201
_D_COLUMNS  = ['endpoint', 'name', 'owner', 'command',  'resource_profile', 'project_name', 'public', 'id', 'created', 'updated', 'state', 'project_id', 'url']  # noqa: E241, E201
_J_COLUMNS  = [            'name', 'owner', 'command',  'resource_profile', 'project_name',           'id', 'created', 'updated', 'state', 'project_id', 'url']  # noqa: E241, E201
_C_COLUMNS  = ['id',  'permission', 'type', 'first_name', 'last_name', 'email']  # noqa: E241, E201
_U_COLUMNS  = ['username', 'firstName', 'lastName', 'lastLogin', 'email', 'id']
_T_COLUMNS  = ['name', 'id', 'is_template', 'is_default', 'description', 'download_url', 'owner', 'created', 'updated']
_A_COLUMNS  = ['type', 'status', 'message', 'done', 'owner', 'id', 'description', 'created', 'updated']
_E_COLUMNS  = ['id', 'owner', 'name', 'project_name', 'deployment_id', 'project_id', 'project_url']
_RV_COLUMNS = ['name', 'created', 'commands', 'owner', 'id', 'project_id', 'updated', 'url']
_RP_COLUMNS = ['name', 'description', 'cpu', 'memory', 'gpu']
_ED_COLUMNS = ['id', 'packages', 'name', 'is_default']
_BR_COLUMNS = ['branch', 'sha1']
_CH_COLUMNS = ['path', 'change_type', 'modified', 'conflicted', 'id']
_PD_COLUMNS = ['name', 'owner', 'type', 'usage/mem', 'usage/cpu', 'usage/gpu', 'node', 'rst', 'phase', 'since', 'resource_profile', 'id']
_DTYPES = {'created': 'datetime', 'updated': 'datetime',
           'since': 'datetime', 'mtime': 'datetime', 'timestamp': 'datetime',
           'createdTimestamp': 'timestamp/ms', 'notBefore': 'timestamp/s',
           'lastLogin': 'timestamp/ms', 'time': 'timestamp/ms'}


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
        if sys.meta_path is not None and not self.persist and self.connected:
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

    def _format_table(self, response, columns, quiet=False):
        if isinstance(response, dict):
            is_series = True
            response = [response]
        elif isinstance(response, list) and all(isinstance(x, dict) for x in response):
            is_series = False
        elif quiet:
            return response
        else:
            raise ValueError('Not a tabular data format')
        clist = list(columns or ())
        if not response:
            return response, clist
        csrc = set(clist)
        for rec in response:
            clist.extend(c for c in rec if c not in csrc)
            csrc.update(rec)
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
        assert not isinstance(response, requests.models.Response)
        if record_type is not None:
            for rec in ([response] if isinstance(response, dict) else response):
                rec['_record_type'] = record_type
        if format in ('table', 'tableif'):
            return self._format_table(response, columns, quiet=format == 'tableif')
        elif format == 'dataframe':
            records, columns = self._format_table(response, columns)
            try:
                import pandas as pd
            except ImportError:
                raise ImportError('Pandas must be installed in order to use format="dataframe"')
            return pd.DataFrame(records, columns=columns)
        else:
            return response

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
        if k8s_endpoint and k8s_endpoint.startswith('ssh:'):
            self._k8s_client = AE5K8SLocalClient(hostname, k8s_endpoint.split(':', 1)[1])
        else:
            k8s_endpoint = k8s_endpoint or DEFAULT_K8S_ENDPOINT
            try:
                response = self._head(f'/_errors/404.html', subdomain=k8s_endpoint, format='response')
                self._k8s_client = AE5K8SRemoteClient(self, k8s_endpoint)
            except AEUnexpectedResponseError:
                self._k8s_client = None

    def _k8s(self, method, *args, **kwargs):
        quiet = kwargs.pop('quiet', False)
        if self._k8s_client is None:
            if not quiet:
                raise AEException('No kubectl connection has been established')
        elif not self._k8s_client.healthy():
            if not quiet:
                raise AEException('Error establishing kubectl connection')
            self._k8s_client = None
        else:
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
        if isinstance(records, dict):
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
        return records

    def _get_records(self, endpoint, filter=None, **kwargs):
        record_type = kwargs.pop('record_type', None)
        if not record_type:
            record_type = endpoint.rsplit('/', 1)[-1].rstrip('s')
        try:
            records = self._get(endpoint, **kwargs)
        except AEUnexpectedResponseError:
            records = []
        records = self._fix_records(records, filter, record_type)
        return records

    def _post_record(self, endpoint, **kwargs):
        filter = kwargs.pop('filter', {})
        record_type = kwargs.pop('record_type', None)
        if not record_type:
            record_type = endpoint.rsplit('/', 1)[-1].rstrip('s')
        records = self._post(endpoint, **kwargs)
        records = self._fix_records(records, filter, record_type)
        return records[0] if records else None

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
        if 'pid' in filter and type == 'projects':
            if filter.get('id') != filter['pid']:
                raise ValueError(f'Expected a {type} ID type, found a {idtype} ID: {ident}')
            del filter['pid']
        if 'id' in filter:
            idtype = ident.id_type(filter['id'])
            tval = 'deployments' if type in ('jobs', 'runs') else type
            if idtype == 'projects' and type != 'projects':
                filter['pid'] = filter['id']
                del filter['id']
            elif type != 'pods' and idtype != tval:
                raise ValueError(f'Expected a {type} ID type, found a {idtype} ID: {ident}')
        return filter

    def _should_be_one(self, matches, type, ident, quiet):
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
        if isinstance(ident, dict) and ident.get('_record_type', '') + 's' == type:
            return ident
        filter = self._id_filter(ident, type)
        if 'id' in filter:
            url = f'{type}/{filter["id"]}'
            del filter['id']
        else:
            url = type
        matches = self._get_records(url, filter)
        return self._should_be_one(matches, type, ident, quiet)

    def _id_or_name(self, type, ident, quiet=False):
        matches = []
        records = getattr(self, type.rstrip('s') + '_list')(internal=True)
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
            raise ValueError(msg)

    def project_list(self, filter=None, collaborators=False, format=None):
        records = self._get_records('projects', filter=filter)
        headers = _P_COLUMNS
        if collaborators:
            self._join_collaborators('projects', records)
            headers = _P_COLUMNS[:4] + ['collaborators'] + _P_COLUMNS[4:]
        return self._format_response(records, format=format, columns=headers)

    def project_info(self, ident, internal=False, collaborators=False, format=None, quiet=False):
        # We're hitting the list endpoint instead of the single-record endpoint because
        # for some reason the individual records don't return project_create_status
        record = self.project_list(filter=ident)
        record = self._should_be_one(record, 'projects', ident, quiet)
        if record:
            headers = _P_COLUMNS
            if not internal and collaborators:
                self._join_collaborators('projects', record)
                headers = _P_COLUMNS[:4] + ['collaborators'] + _P_COLUMNS[4:]
            return self._format_response(record, format=format, columns=headers)

    def resource_profile_list(self, internal=False, format=None):
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
        return self._format_response(profiles, format=format, columns=_RP_COLUMNS)

    def resource_profile_info(self, name, internal=False, format=None, quiet=False):
        record = self._id_or_name('resource_profile', name, quiet=quiet)
        return self._format_response(record, format=format, columns=_RP_COLUMNS)

    def editor_list(self, internal=False, format=None):
        response = self._get('projects/actions', params={'q': 'create_action'})[0]
        editors = response['editors']
        for rec in editors:
            rec['packages'] = ' '.join(rec['packages'])
            rec['_record_type'] = 'editor'
        return self._format_response(editors, format=format, columns=_ED_COLUMNS)

    def editor_info(self, name, internal=False, format=None, quiet=False):
        record = self._id_or_name('editor', name)
        return self._format_response(record, format=format, columns=_ED_COLUMNS)

    def sample_list(self, internal=False, format=None):
        result = []
        for sample in self._get_records('template_projects', record_type='template'):
            sample['is_template'] = True
            result.append(sample)
        for sample in self._get_records('sample_projects', record_type='template'):
            sample['is_template'] = sample['is_default'] = False
            result.append(sample)
        return self._format_response(result, format=format, columns=_T_COLUMNS)

    def sample_info(self, ident, internal=False, format=None, quiet=False):
        record = self._id_or_name('sample', ident, quiet=quiet)
        return self._format_response(record, format=format, columns=_T_COLUMNS, record_type='template')

    def sample_clone(self, ident, name=None, tag=None,
                     make_unique=None, wait=True, format=None):
        record = self._id_or_name('sample', ident)
        if name is None:
            name = record['name']
            if make_unique is None:
                make_unique = True
        return self.project_create(record['download_url'], name=name, tag=tag,
                                   make_unique=make_unique, wait=wait, format=format)

    def project_collaborator_list(self, ident, format=None):
        prec = self._id('projects', ident)
        record = self._get_records(f'projects/{prec["id"]}/collaborators')
        return self._format_response(record, format=format, columns=_C_COLUMNS)

    def project_collaborator_info(self, ident, userid, internal=False, format=None, quiet=False):
        collabs = self.project_collaborator_list(ident)
        for c in collabs:
            if userid == c['id']:
                return self._format_response(c, format=format, columns=_C_COLUMNS)
        if not quiet:
            raise AEException(f'Collaborator not found: {userid}')

    def project_collaborator_list_set(self, ident, collabs, format=None):
        prec = self._id('projects', ident)
        result = self._put_record(f'projects/{prec["id"]}/collaborators', json=collabs)
        return self._format_response(result['collaborators'], format=format, columns=_C_COLUMNS)

    def project_collaborator_add(self, ident, userid, group=False, read_only=False, format=None):
        prec = self._id('projects', ident)
        collabs = self.project_collaborator_list(prec["id"])
        ncollabs = len(collabs)
        if not isinstance(userid, tuple):
            userid = userid,
        collabs = [c for c in collabs if c['id'] not in userid]
        if len(collabs) != ncollabs:
            self.project_collaborator_list_set(id, collabs)
        type = 'group' if group else 'user'
        perm = 'r' if read_only else 'rw'
        collabs.extend({'id': u, 'type': type, 'permission': perm} for u in userid)
        return self.project_collaborator_list_set(prec, collabs, format=format)

    def project_collaborator_remove(self, ident, userid, format=None):
        prec = self._id('projects', ident)
        collabs = self.project_collaborator_list(prec["id"])
        if not isinstance(userid, tuple):
            userid = userid,
        missing = set(userid) - set(c['id'] for c in collabs)
        if missing:
            missing = ', '.join(missing)
            raise AEException(f'Collaborator(s) not found: {missing}')
        collabs = [c for c in collabs if c['id'] not in userid]
        return self.project_collaborator_list_set(prec, collabs, format=format)

    def project_patch(self, ident, **kwargs):
        format = kwargs.pop('format', None)
        prec = self._id('projects', ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            self._patch(f'projects/{prec["id"]}', json=data)
        return self.project_info(prec["id"], format=format)

    def project_sessions(self, ident, format=None):
        prec = self._id('projects', ident)
        record = self._get(f'projects/{prec["id"]}/sessions')
        return self._format_response(record, format=format, columns=_D_COLUMNS, record_type='deployment')

    def project_deployments(self, ident, format=None):
        prec = self._id('projects', ident)
        response = self._get(f'projects/{prec["id"]}/deployments')
        self._fix_endpoints(response)
        return self._format_response(response, format=format, columns=_D_COLUMNS, record_type='deployment')

    def project_jobs(self, ident, format=None):
        prec = self._id('projects', ident)
        response = self._get(f'projects/{prec["id"]}/jobs')
        return self._format_response(response, format=format, columns=_J_COLUMNS, record_type='job')

    def project_runs(self, ident, format=None):
        prec = self._id('projects', ident)
        response = self._get(f'projects/{prec["id"]}/runs')
        return self._format_response(response, format=format, columns=_J_COLUMNS, record_type='run')

    def project_activity(self, ident, limit=0, latest=False, format=None):
        prec = self._id('projects', ident)
        limit = 1 if latest else (999999 if limit <= 0 else limit)
        params = {'sort': '-updated', 'page[size]': limit}
        response = self._get_records(f'projects/{prec["id"]}/activity', params=params)
        if latest:
            response = response[0]
        return self._format_response(response, format=format, columns=_A_COLUMNS, record_type='activity')

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
            return None, None
        filter = {'name': revision} if revision and revision != 'latest' else {}
        response = self._get_records(f'projects/{prec["id"]}/revisions', filter)
        if revision == 'latest' or not revision and latest:
            response = [rec for rec in response if rec['latest']]
            if latest == 'keep' and response:
                response[0]['name'] == 'latest'
        elif not revision and latest:
            response = [response[0]]
        for rec in response:
            rec['project_id'] = prec["id"]
            rec['_project'] = prec
        return prec, response

    def revision_list(self, filter=None, format=None):
        _, response = self._revisions(filter, latest=False, quiet=False)
        return self._format_response(response, format=format, columns=_RV_COLUMNS)

    def _revision(self, ident, keep_latest=True, quiet=False):
        latest = 'keep' if keep_latest else True
        prec, response = self._revisions(ident, latest=latest, quiet=quiet)
        if prec:
            response = self._should_be_one(response, 'revisions', ident, quiet)
        if response is None:
            prec = None
        return prec, response

    def revision_info(self, ident, internal=False, format=None, quiet=False):
        prec, rrec = self._revision(ident, quiet=quiet)
        return self._format_response(rrec, format=format, columns=_RV_COLUMNS)

    def project_download(self, ident, filename=None):
        prec, rrec = self._revision(ident)
        response = self._get(f'projects/{prec["id"]}/revisions/{rrec["id"]}/archive', format='blob')
        if filename is None:
            return response
        with open(filename, 'wb') as fp:
            fp.write(response)

    def project_image(self, ident, command=None, condarc_path=None, dockerfile_path=None, debug=False, format=None):
        '''Build docker image'''
        prec, rrec = self._revision(ident)
        name = prec['name'].replace(' ','').lower()
        owner = prec['owner'].replace('@','_at_')
        tag = f'{owner}/{name}:{rrec["name"]}'

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

    def project_delete(self, ident, format=None):
        prec = self._id('projects', ident)
        return self._delete(f'projects/{prec["id"]}', format=format or 'response')

    def _wait(self, id, status):
        index = 0
        while not status['done'] and not status['error']:
            time.sleep(5)
            params = {'sort': '-updated', 'page[size]': index + 1}
            activity = self._get_records(f'projects/{id}/activity', params=params)
            try:
                status = next(s for s in activity if s['id'] == status['id'])
            except StopIteration:
                index = index + 1
        return status

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
        if not wait:
            return self._format_response(response, format, columns=_P_COLUMNS)
        response['action'] = self._wait(response['id'], response['action'])
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
        if not wait:
            return self._format_response(response, format, columns=_P_COLUMNS)
        response['action'] = self._wait(response['id'], response['action'])
        if response['action']['error']:
            raise RuntimeError('Error processing upload: {}'.format(response['action']['message']))
        return self.project_info(response['id'], format=format)

    def _join_collaborators(self, what, response):
        if isinstance(response, dict):
            collabs = self._get(f'{what}/{response["id"]}/collaborators')
            response['collaborators'] = ', '.join(c['id'] for c in collabs)
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
        if isinstance(response, list) and len(response) == 1:
            response = response[0]
        if isinstance(response, dict):
            pid = 'a0-' + response['project_url'].rsplit('/', 1)[-1]
            project = self._get(f'projects/{pid}')
            response['session_name'] = response['name']
            response['name'] = project['name']
            response['project_id'] = pid
        elif response:
            pnames = {x['id']: x['name'] for x in self._get('projects')}
            for rec in response:
                pid = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
                pname = pnames.get(pid, '')
                rec['session_name'] = rec['name']
                rec['name'] = pname
                rec['project_id'] = pid

    def session_list(self, filter=None, k8s=False, format=None):
        records = self._get_records('sessions', filter=filter)
        headers = _S_COLUMNS
        if k8s:
            nhead = self._join_k8s(records, True)
            headers = headers[:2] + nhead + headers[2:]
        return self._format_response(records, format, columns=headers)

    def session_info(self, ident, internal=False, k8s=False, format=None, quiet=False):
        record = self._id('sessions', ident, quiet=quiet)
        headers = _S_COLUMNS
        if not internal and k8s:
            nhead = self._join_k8s(record, True)
            headers = headers[:2] + nhead + headers[2:]
        return self._format_response(record, format, columns=headers)

    def session_changes(self, ident, master=False, format=None):
        srec = self._id('sessions', ident)
        which = 'master' if master else 'local'
        result = self._get(f'sessions/{srec["id"]}/changes/{which}')
        return self._format_response(result['files'], format=format, columns=_CH_COLUMNS, record_type='changes')

    def session_branches(self, ident, format=None):
        srec = self._id('sessions', ident)
        # Use master because it's more likely to be a smaller result (no changed files)
        result = self._get(f'sessions/{srec["id"]}/changes/master')
        result = [{'branch': k, 'sha1': v, '_record_type': 'branch'} for k, v in result['branches'].items()]
        return self._format_response(result, format=format, columns=_BR_COLUMNS)

    def session_start(self, ident, editor=None, resource_profile=None, wait=True, format=None):
        prec = self._id('projects', ident)
        patches = {}
        for key, value in (('editor', editor), ('resource_profile', resource_profile)):
            if value and prec.get(key) != value:
                patches[key] = value
        if patches:
            self._patch(f'projects/{prec["id"]}', json=patches)
        response = self._post_record(f'projects/{prec["id"]}/sessions')
        if response.get('error'):
            raise RuntimeError('Error starting project: {}'.format(response['error']['message']))
        if wait:
            response['action'] = self._wait(id, response['action'])
        if response['action'].get('error'):
            raise RuntimeError('Error completing session start: {}'.format(response['action']['message']))
        return self._format_response(response, format=format, columns=_S_COLUMNS)

    def session_stop(self, ident, format=format):
        srec = self._id('sessions', ident)
        self._delete(f'sessions/{srec["id"]}')

    def session_restart(self, ident, wait=True, format=None):
        srec = self._id('sessions', ident)
        self._delete(f'sessions/{srec["id"]}')
        # Unlike deployments I am not copying over the editor and resource profile
        # settings from the current session. That's because I want to support the use
        # case where the session settings are patched prior to restart
        return self.session_start(srec['project_id'], wait=wait, format=format)

    def _fix_deployments(self, records):
        # Add the project ID to the deployment record
        for record in ([records] if isinstance(records, dict) else records):
            pid = 'a0-' + record['project_url'].rsplit('/', 1)[-1]
            record['project_id'] = pid
            if record.get('url'):
                record['endpoint'] = record['url'].split('/', 3)[2].split('.', 1)[0]

    def deployment_list(self, filter=None, collaborators=True, endpoints=True, k8s=False, format=None):
        response = self._get_records('deployments', filter=filter)
        if collaborators:
             self._join_collaborators('deployments', response)
        headers = _D_COLUMNS
        if k8s:
            nhead = self._join_k8s(response, False)
            headers = headers[:3] + nhead + headers[3:]
        return self._format_response(response, format, headers)

    def deployment_info(self, ident, collaborators=True, k8s=False, format=None, quiet=False):
        record = self._id('deployments', ident, quiet=quiet)
        if record:
            if collaborators:
                self._join_collaborators('deployments', record)
            headers = _D_COLUMNS
            if k8s:
                nhead = self._join_k8s(record, False)
                headers = headers[:3] + nhead + headers[3:]
            return self._format_response(record, format, headers)

    def endpoint_list(self, format=None, internal=False):
        response = self._get_records('/platform/deploy/api/v1/apps/static-endpoints', record_type='endpoint')
        deps = self.deployment_list(collaborators=False, k8s=False)
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
                newrecs.append(rec)
        newrecs = self._id_filter(newrecs, filter)
        return self._format_response(newrecs, format=format, columns=_E_COLUMNS)

    def endpoint_info(self, ident, internal=False, format=None, quiet=False):
        record = self._id_or_name('endpoint', ident, quiet=quiet)
        return self._format_response(record, format=format, columns=_E_COLUMNS, record_type='endpoint')

    def deployment_collaborator_list(self, ident, format=None):
        drec = self._id('deployments', ident)
        response = self._get_records(f'deployments/{drec["id"]}/collaborators')
        return self._format_response(response, format=format, columns=_C_COLUMNS)

    def deployment_collaborator_info(self, ident, userid, internal=False, format=None, quiet=False):
        collabs = self.deployment_collaborator_list(ident)
        for c in collabs:
            if userid == c['id']:
                return self._format_response(c, format=format, columns=_C_COLUMNS)
        if not quiet:
            raise AEException(f'Collaborator not found: {userid}')

    def deployment_collaborator_list_set(self, ident, collabs, format=None):
        drec = self._id('deployments', ident)
        result = self._put(f'deployments/{drec["id"]}/collaborators', json=collabs)
        return self._format_response(result['collaborators'], format=format, columns=_C_COLUMNS, record_type='collaborators')

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
                         stop_on_error=False, format=None):
        prec, rrec = self._revision(ident, latest='keep')
        data = {'source': rrec['url'],
                'revision': rrec['id'],
                'resource_profile': resource_profile or prec['resource_profile'],
                'command': command or rrec['commands'][0]['id'],
                'public': bool(public),
                'target': 'deploy'}
        if name:
            data['name'] = name
        if endpoint:
            try:
                self._head(f'/_errors/404.html', subdomain=endpoint)
                raise AEException('endpoint "{}" is already in use'.format(endpoint))
            except AEUnexpectedResponseError:
                pass
            data['static_endpoint'] = endpoint
        response = self._post_record(f'projects/{prec["id"]}/deployments', json=data)
        if response.get('error'):
            raise AEException('Error starting deployment: {}'.format(response['error']['message']))
        if collaborators:
            self.deployment_collaborator_list_set(response['id'], collaborators)
        # The _wait method doesn't work here. The action isn't even updated, it seems
        if wait or stop_on_error:
            while response['state'] in ('initial', 'starting'):
                time.sleep(5)
                response = self._get(f'deployments/{response["id"]}', record_type='deployment')
            if response['state'] != 'started':
                if stop_on_error:
                    self.deployment_stop(response["id"])
                raise AEException(f'Error completing deployment start: {response["status_text"]}')
        response['project_id'] = id
        return self._format_response(response, format=format, columns=_D_COLUMNS)

    def deployment_restart(self, ident, wait=True, stop_on_error=False, format=None):
        drec = self._id('deployments', ident)
        collab = self.deployment_collaborators(drec)
        if record.get('url'):
            endpoint = record['url'].split('/', 3)[2].split('.', 1)[0]
            if id.endswith(endpoint):
                endpoint = None
        else:
            endpoint = None
        self._delete(f'deployments/{drec["id"]}')
        return self.deployment_start(record['project_id'],
                                     endpoint=endpoint, command=record['command'],
                                     resource_profile=record['resource_profile'], public=record['public'],
                                     collaborators=collab, wait=wait,
                                     stop_on_error=stop_on_error, format=format)

    def deployment_patch(self, ident, format=None, **kwargs):
        drec = self._id('deployments', ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            self._patch(f'deployments/{drec["id"]}', json=data)
            drec = self.deployment_info(drec["id"])
        return self._format_response(drec, format=format)

    def deployment_stop(self, ident, format=None):
        drec = self._id('deployments', ident)
        response = self._delete(f'deployments/{drec["id"]}')

    def deployment_logs(self, ident, which=None, format=None):
        drec = self._id('deployments', ident)
        result = self._get(f'deployments/{drec["id"]}/logs')
        if which is not None:
            result = result[which]
        return self._format_response(result, format=format)

    def deployment_token(self, ident, which=None, format=None):
        drec = self._id('deployments', ident)
        result = self._post(f'deployments/{drec["id"]}/token', format='json')
        if isinstance(result, dict) and set(result) == {'token'}:
            result = result['token']
        return self._format_response(result, format=format)

    def job_list(self, filter=None, internal=False, format=None):
        response = self._get_records('jobs')
        return self._format_response(response, format=format, columns=_J_COLUMNS)

    def job_info(self, ident, internal=False, format=None, quiet=False):
        jrec = self._id('jobs', ident, quiet=quiet)
        return self._format_response(jrec, format=format, columns=_J_COLUMNS)

    def job_runs(self, ident, format=None):
        jrec = self._id('jobs', ident)
        response = self._get_records(f'jobs/{jrec["id"]}/runs')
        return self._format_response(response, format=format, columns=_J_COLUMNS)

    def job_delete(self, ident, format=None):
        jrec = self._id('jobs', ident)
        response = self._delete(f'jrec/{jrec["id"]}')
        return self._format_response(response, format=format)

    def job_pause(self, ident, format=None):
        jrec = self._id('jobs', ident)
        response = self._post(f'jobs/{jrec["id"]}/pause')
        return self._format_response(response, format=format, columns=_J_COLUMNS, record_type='job')

    def job_unpause(self, ident, format=format):
        jrec = self._id('jobs', ident)
        response = self._post(f'jobs/{jrec["id"]}/unpause')
        return self._format_response(response, format=format, columns=_J_COLUMNS, record_type='job')

    def job_create(self, ident, schedule=None, name=None, command=None,
                   resource_profile=None, variables=None, run=False,
                   wait=False, cleanup=False, make_unique=None,
                   show_run=False, format=None):
        if cleanup and schedule:
            raise ValueError('cannot use cleanup=True with a scheduled job')
        if cleanup and (not run or not wait):
            raise ValueError('must specify run=wait=True with cleanup=True')
        prec, rrec = self._revision(ident, latest='keep')
        if not command:
            command = rrec['commands'][0]['id']
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
                'resource_profile': resource_profile or prec['resource_profile'],
                'command': command,
                'target': 'deploy',
                'schedule': schedule,
                'autorun': run,
                'revision': rrec['name'],
                'name': name}
        if variables:
            data['variables'] = variables
        response = self._post_record(f'projects/{id}/jobs', json=data, record_type='job')
        if response.get('error'):
            raise AEException('Error starting job: {}'.format(response['error']['message']))
        response['project_id'] = id
        if run:
            run = self._get_records(f'jobs/{response["id"]}/runs')[-1]
            if wait:
                while run['state'] not in ('completed', 'error'):
                    time.sleep(5)
                    run = self._get(f'runs/{run["id"]}')
                if cleanup:
                    self._delete(f'jobs/{response["id"]}')
            if show_run:
                response = run
        return self._format_response(response, format=format, columns=_J_COLUMNS)

    def job_patch(self, ident, name=None, command=None, schedule=None,
                  resource_profile=None, variables=None, format=None):
        jrec = self._id('jobs', ident)
        data = {}
        if name:
            data['name'] = name
        if command:
            data['command'] = command
        if schedule:
            data['schedule'] = schedule
        if resource_profile:
            data['resource_profile'] = resource_profile
        if variables is not None:
            data['variables'] = variables
        response = self._patch_record(f'jobs/{jrec["id"]}', json=data, record_type='job')
        return self._format_response(response, format=format, columns=_J_COLUMNS)

    def run_list(self, filter=None,  internal=False, format=None):
        response = self._get_records('runs', filter=filter)
        return self._format_response(response, format=format, columns=_J_COLUMNS)

    def run_info(self, ident, internal=False, format=None, quiet=False):
        rrec = self._id('runs', ident, quiet=quiet)
        if rrec:
            return self._format_response(rrec, format=format, columns=_J_COLUMNS)

    def run_log(self, ident, format=None):
        rrec = self._id('runs', ident)
        return self._get(f'runs/{rrec["id"]}/logs')['job']

    def run_stop(self, ident, format=None):
        rrec = self._id('runs', ident)
        response = self._post(f'runs/{rrec["id"]}/stop')
        return self._format_response(response, format=format, columns=_J_COLUMNS, record_type='job')

    def run_delete(self, ident, format=None):
        rrec = self._id('runs', ident)
        self._delete(f'runs/{rrec["id"]}')

    def pod_list(self, filter=None, internal=False, format=None):
        records = []
        for type in ('session', 'deployment', 'run'):
            for rec in getattr(self, f'{type}_list')(format='json', internal=True):
                value = {k: rec[k] for k in ('name', 'owner', 'resource_profile', 'id')}
                value['type'] = type[:4]
                value['_record_type'] = 'pod'
                records.append(value)
        if not internal:
            self._join_k8s(records, True)
        return self._format_response(records, format=format, columns=_PD_COLUMNS)

    def pod_info(self, pod, format=None):
        record = self._id('pods', pod)
        self._join_k8s(record, True)
        return self._format_response(record, format=format, columns=_PD_COLUMNS)

    def node_list(self, internal=False, format=None):
        records = self._k8s('node_info')
        result = []
        for rec in records:
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

    def node_info(self, node, format=None, quiet=False):
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
        return self._format_response(users, format=format, columns=_U_COLUMNS, record_type='user')

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
        return self._format_response(response, format, columns=_U_COLUMNS, record_type='user')

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
