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

from http.cookiejar import LWPCookieJar
from requests.packages import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Maximum page size in keycloak
KEYCLOAK_PAGE_MAX = os.environ.get('KEYCLOAK_PAGE_MAX', 1000)


_P_COLUMNS = [            'name', 'owner', 'editor',              'resource_profile',                           'id', 'created', 'updated', 'project_create_status',               'url']  # noqa: E241, E201
_R_COLUMNS = [            'name', 'owner', 'commands',                                                          'id', 'created', 'updated',                          'project_id', 'url']  # noqa: E241, E201
_S_COLUMNS = [            'name', 'owner', 'changes', 'modified', 'resource_profile',                           'id', 'created', 'updated', 'state',                 'project_id', 'url']  # noqa: E241, E201
_D_COLUMNS = ['endpoint', 'name', 'owner', 'command',             'resource_profile', 'project_name', 'public', 'id', 'created', 'updated', 'state',                 'project_id', 'url']  # noqa: E241, E201
_J_COLUMNS = [            'name', 'owner', 'command',             'resource_profile', 'project_name',           'id', 'created', 'updated', 'state',                 'project_id', 'url']  # noqa: E241, E201
_C_COLUMNS = ['id',  'permission', 'type', 'first name', 'last name', 'email']  # noqa: E241, E201
_U_COLUMNS = ['username', 'firstName', 'lastName', 'lastLogin', 'email', 'id']
_T_COLUMNS = ['name', 'id', 'description', 'is_template', 'is_default', 'download_url', 'owner', 'created', 'updated']
_A_COLUMNS = ['type', 'status', 'message', 'done', 'owner', 'id', 'description', 'created', 'updated']
_E_COLUMNS = ['id', 'owner', 'name', 'deployment_id', 'project_name', 'project_id', 'project_url']
_R_COLUMNS = ['name', 'description', 'cpu', 'memory', 'gpu']
_ED_COLUMNS = ['id', 'packages', 'name', 'is_default']
_BR_COLUMNS = ['branch', 'sha1']
_CH_COLUMNS = ['path', 'change_type', 'modified', 'conflicted', 'id']
_DTYPES = {'created': 'datetime', 'updated': 'datetime', 'modified': 'datetime',
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

    def _format_kwargs(self, kwargs):
        return kwargs.pop('format', None), kwargs.pop('columns', None)

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
        csrc = set()
        cdst = set(clist)
        for rec in response:
            clist.extend(c for c in rec if c not in cdst)
            cdst.update(rec)
            csrc.update(rec)
        clist = [c for c in clist if c in csrc]
        for col, dtype in _DTYPES.items():
            if col in cdst:
                if dtype == 'datetime':
                    for rec in response:
                        if rec.get(col):
                            rec[col] = parser.isoparse(rec[col])
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

    def _format_response(self, response, format, columns=None):
        if isinstance(response, requests.models.Response):
            if format == 'response':
                return response
            if len(response.content) == 0:
                return None
            if format == 'blob':
                return response.content
            if format == 'text':
                return response.text
            ctype = response.headers['content-type']
            if ctype.endswith('json'):
                response = response.json()
            elif format in ('json', 'table'):
                raise AEException(f'Content type {ctype} not compatible with json format')
            else:
                return response.text
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
        fmt, cols = self._format_kwargs(kwargs)
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
                return self._format_response(response, fmt, cols)

    def _get(self, endpoint, **kwargs):
        return self._api('get', endpoint, **kwargs)

    def _delete(self, endpoint, **kwargs):
        return self._api('delete', endpoint, **kwargs)

    def _post(self, endpoint, **kwargs):
        return self._api('post', endpoint, **kwargs)

    def _put(self, endpoint, **kwargs):
        return self._api('put', endpoint, **kwargs)

    def _patch(self, endpoint, **kwargs):
        return self._api('patch', endpoint, **kwargs)


class AEUserSession(AESessionBase):
    def __init__(self, hostname, username, password=None, persist=True):
        self._filename = os.path.join(config._path, 'cookies', f'{username}@{hostname}')
        super(AEUserSession, self).__init__(hostname, username, password=password,
                                            prefix='api/v2', persist=persist)

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

    def _id(self, type, ident, quiet=False):
        if isinstance(ident, str):
            ident = Identifier.from_string(ident, no_revision=type != 'projects')
        tval = 'deployments' if type in ('jobs', 'runs') else type
        idtype = ident.id_type(ident.id) if ident.id else tval
        if idtype not in ('projects', tval):
            raise ValueError(f'Expected a {type} ID type, found a {idtype} ID: {ident}')
        matches = []
        # NOTE: we are retrieving all project records here, even if we have the unique
        # id and could potentially retrieve the individual record, because the full
        # listing includes a field the individual query does not (project_create_status)
        # Also, we're using our wrapper around the list API calls instead of the direct
        # call so we get the benefit of our record cleanup.
        records = getattr(self, type.rstrip('s') + '_list')(internal=True)
        owner, name, id, pid = (ident.owner or '*', ident.name or '*',
                                ident.id if ident.id and tval == idtype else '*',
                                ident.pid if ident.pid and type != 'projects' else '*')
        for rec in records:
            if (fnmatch(rec['owner'], owner) and fnmatch(rec['name'], name) and
                fnmatch(rec['id'], id) and fnmatch(rec.get('project_id', ''), pid)): # noqa
                matches.append(rec)
        if len(matches) == 1:
            rec = matches[0]
            id = rec['id']
        elif quiet:
            id, rec = None, None
        else:
            pfx = 'Multiple' if len(matches) else 'No'
            msg = f'{pfx} {type} found matching {owner}/{name}/{id}'
            if matches:
                matches = [str(Identifier.from_record(r, True)) for r in matches]
                msg += ':\n  - ' + '\n  - '.join(matches)
            raise ValueError(msg)
        return rec['id'], rec

    def _revision(self, ident, keep_latest=False, quiet=False):
        if isinstance(ident, str):
            ident = Identifier.from_string(ident)
        id, prec = self._id('projects', ident, quiet=quiet)
        rrec, rev = None, None
        if id:
            revisions = self._get(f'projects/{id}/revisions')
            if not ident.revision or ident.revision == 'latest':
                matches = [revisions[0]]
            else:
                matches = []
                for response in revisions:
                    if fnmatch(response['name'], ident.revision):
                        matches.append(response)
            if len(matches) == 1:
                rrec = matches[0]
                if not keep_latest or (ident.revision and ident.revision != 'latest'):
                    rev = rrec['name']
            elif not quiet:
                pfx = 'Multiple' if len(matches) else 'No'
                msg = f'{pfx} revisions found matching {ident.revision}'
                if matches:
                    msg += ':\n  - ' + '\n  - '.join(matches)
                raise ValueError(msg)
        return id, rev, prec, rrec

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
            rec = matches[0]
            id = rec.get('id', rec['name'])
        elif quiet:
            id, rec = None, None
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
        return id, rec

    def project_list(self, collaborators=False, internal=False, format=None):
        records = self._get('projects')
        if collaborators and not internal:
            self._join_collaborators('projects', records)
            columns = list(_P_COLUMNS)
            columns.insert(4, 'collaborators')
        else:
            columns = _P_COLUMNS
        return self._format_response(records, format=format, columns=columns)

    def project_info(self, ident, collaborators=True, internal=False, format=None, quiet=False):
        id, record = self._id('projects', ident, quiet=quiet)
        if record and (collaborators and not internal):
            self._join_collaborators('projects', record)
            columns = list(_P_COLUMNS)
            columns.insert(4, 'collaborators')
        else:
            columns = _P_COLUMNS
        return self._format_response(record, format=format, columns=columns)

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
        return self._format_response(profiles, format=format, columns=_R_COLUMNS)

    def resource_profile_info(self, name, internal=False, format=None, quiet=False):
        id, rec = self._id_or_name('resource_profile', name, quiet=quiet)
        return self._format_response(rec, format=format, columns=_R_COLUMNS)

    def editor_list(self, internal=False, format=None):
        response = self._get('projects/actions', params={'q': 'create_action'})[0]
        editors = response['editors']
        for rec in editors:
            rec['packages'] = ' '.join(rec['packages'])
        return self._format_response(editors, format=format, columns=_ED_COLUMNS)

    def editor_info(self, name, internal=False, format=None, quiet=False):
        id, rec = self._id_or_name('editor', name)
        return self._format_response(rec, format=format, columns=_ED_COLUMNS)

    def sample_list(self, internal=False, format=None):
        result = []
        for sample in self._get('template_projects'):
            sample['is_template'] = True
            result.append(sample)
        for sample in self._get('sample_projects'):
            sample['is_template'] = sample['is_default'] = False
            result.append(sample)
        return self._format_response(result, format=format, columns=_T_COLUMNS)

    def sample_info(self, ident, internal=False, format=None, quiet=False):
        id, record = self._id_or_name('sample', ident, quiet=quiet)
        return self._format_response(record, format=format, columns=_T_COLUMNS)

    def sample_clone(self, ident, name=None, tag=None,
                     make_unique=None, wait=True, format=None):
        id, record = self._id_or_name('sample', ident)
        if name is None:
            name = record['name']
            if make_unique is None:
                make_unique = True
        return self.project_create(record['download_url'], name=name, tag=tag,
                                   make_unique=make_unique, wait=wait, format=format)

    def project_collaborator_list(self, ident, format=None):
        id, _ = self._id('projects', ident)
        return self._get(f'projects/{id}/collaborators', format=format, columns=_C_COLUMNS)

    def project_collaborator_info(self, ident, userid, internal=False, format=None, quiet=False):
        collabs = self.project_collaborator_list(ident)
        for c in collabs:
            if userid == c['id']:
                return self._format_response(c, format=format, columns=_C_COLUMNS)
        if not quiet:
            raise AEException(f'Collaborator not found: {userid}')

    def project_collaborator_list_set(self, ident, collabs, format=None):
        id, _ = self._id('projects', ident)
        result = self._put(f'projects/{id}/collaborators', json=collabs)
        return self._format_response(result['collaborators'], format=format, columns=_C_COLUMNS)

    def project_collaborator_add(self, ident, userid, group=False, read_only=False):
        id, _ = self._id('projects', ident)
        collabs = self.project_collaborator_list(id)
        ncollabs = len(collabs)
        if not isinstance(userid, tuple):
            userid = userid,
        collabs = [c for c in collabs if c['id'] not in userid]
        if len(collabs) != ncollabs:
            self.project_collaborator_list_set(id, collabs)
        type = 'group' if group else 'user'
        perm = 'r' if read_only else 'rw'
        collabs.extend({'id': u, 'type': type, 'permission': perm} for u in userid)
        return self.project_collaborator_list_set(id, collabs, format=format)

    def project_collaborator_remove(self, ident, userid, format=None):
        id, _ = self._id('projects', ident)
        collabs = self.project_collaborator_list(id)
        if not isinstance(userid, tuple):
            userid = userid,
        missing = set(userid) - set(c['id'] for c in collabs)
        if missing:
            missing = ', '.join(missing)
            raise AEException(f'Collaborator(s) not found: {missing}')
        collabs = [c for c in collabs if c['id'] not in userid]
        return self.project_collaborator_list_set(id, collabs, format=format)

    def project_patch(self, ident, **kwargs):
        format = kwargs.pop('format', None)
        id, _ = self._id('projects', ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            self._patch(f'projects/{id}', json=data)
        return self.project_info(id, format=format)

    def project_sessions(self, ident, format=None):
        id, _ = self._id('projects', ident)
        return self._get(f'projects/{id}/sessions', format=format, columns=_S_COLUMNS)

    def project_deployments(self, ident, format=None):
        id, _ = self._id('projects', ident)
        response = self._get(f'projects/{id}/deployments')
        self._fix_endpoints(response)
        return self._format_response(response, format=format, columns=_D_COLUMNS)

    def project_jobs(self, ident, format=None):
        id, _ = self._id('projects', ident)
        return self._get(f'projects/{id}/jobs', format=format, columns=_J_COLUMNS)

    def project_runs(self, ident, format=None):
        id, _ = self._id('projects', ident)
        return self._get(f'projects/{id}/runs', format=format, columns=_R_COLUMNS)

    def project_activity(self, ident, limit=0, latest=False, format=None):
        id, _ = self._id('projects', ident)
        limit = 1 if latest else (999999 if limit <= 0 else limit)
        params = {'sort': '-updated', 'page[size]': limit}
        response = self._get(f'projects/{id}/activity', params=params)['data']
        if latest:
            response = response[0]
        return self._format_response(response, format=format, columns=_A_COLUMNS)

    def revision_list(self, ident, format=None):
        id, _ = self._id('projects', ident)
        response = self._get(f'projects/{id}/revisions')
        for rec in response:
            rec['project_id'] = 'a0-' + rec['url'].rsplit('/', 3)[-3]
        return self._format_response(response, format=format, columns=_R_COLUMNS)

    def revision_info(self, ident, internal=False, format=None, quiet=False):
        id, rev, prec, rrec = self._revision(ident, quiet=quiet)
        if rrec:
            rrec['project_id'] = prec['id']
        return self._format_response(rrec, format=format, columns=_R_COLUMNS)

    def project_download(self, ident, filename=None):
        id, rev, _, _ = self._revision(ident)
        response = self._get(f'projects/{id}/revisions/{rev}/archive', format='blob')
        if filename is None:
            return response
        with open(filename, 'wb') as fp:
            fp.write(response)

    def project_image(self, ident, command=None, condarc_path=None, dockerfile_path=None, debug=False, format=None):
        '''Build docker image'''
        _, rev, _, rrec = self._revision(ident)
        info = self.project_info(ident, format='response')
        name = info['name'].replace(' ','').lower()
        owner = info['owner'].replace('@','_at_')
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

    def project_delete(self, ident, format=None):
        id, _ = self._id('projects', ident)
        return self._delete(f'projects/{id}', format=format or 'response')

    def _wait(self, id, status):
        index = 0
        while not status['done'] and not status['error']:
            time.sleep(5)
            params = {'sort': '-updated', 'page[size]': index + 1}
            activity = self._get(f'projects/{id}/activity', params=params)
            try:
                status = next(s for s in activity['data'] if s['id'] == status['id'])
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
        response = self._post('projects', json=params)
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
                with tarfile.open(fileobj=f, mode='w|gz') as tf:
                    tf.add(project_archive, arcname='project', recursive=True)
                f.seek(0)
            data = {'name': name}
            if tag:
                data['tag'] = tag
            response = self._post('projects/upload', files={'project_file': f}, data=data)
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

    def _join_projects(self, response, nameprefix=None):
        if isinstance(response, dict):
            pid = 'a0-' + response['project_url'].rsplit('/', 1)[-1]
            project = self._get(f'projects/{pid}')
            if nameprefix or 'name' not in response:
                if 'name' in response:
                    response[f'{nameprefix}_name'] = response['name']
                response['name'] = project['name']
            else:
                response['project_name'] = project['name']
            response['project_id'] = pid
        elif response:
            pnames = {x['id']: x['name'] for x in self._get('projects')}
            for rec in response:
                pid = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
                pname = pnames.get(pid, '')
                if nameprefix or 'name' not in rec:
                    if 'name' in rec:
                        rec[f'{nameprefix}_name'] = rec['name']
                    rec['name'] = pname
                else:
                    rec['project_name'] = pname
                rec['project_id'] = pid if pname else ''

    def _join_collaborators(self, what, response):
        if isinstance(response, dict):
            collabs = self._get(f'{what}/{response["id"]}/collaborators')
            response['collaborators'] = ', '.join(c['id'] for c in collabs)
        elif response:
            for rec in response:
                self._join_collaborators(what, rec)

    def _fix_endpoints(self, response):
        if isinstance(response, dict):
            if response.get('url'):
                response['endpoint'] = response['url'].split('/', 3)[2].split('.', 1)[0]
        else:
            for record in response:
                self._fix_endpoints(record)

    def _join_changes(self, record):
        for rec in ([record] if isinstance(record, dict) else record):
            try:
                changes = self.session_changes(rec['id'], format='json')
            except AEException:
                continue
            rec['changes'] = ', '.join(r['path'] for r in changes)
            rec['modified'] = max((r['modified'] or rec['updated'] for r in changes), default='')

    def session_list(self, internal=False, changes=False, format=None):
        response = self._get('sessions')
        # We need _join_projects even in internal mode to replace
        # the internal session name with the project name
        self._join_projects(response, 'session')
        if not internal and changes:
            self._join_changes(response)
        return self._format_response(response, format, columns=_S_COLUMNS)

    def session_info(self, ident, internal=False, changes=False, format=None, quiet=False):
        id, record = self._id('sessions', ident, quiet=quiet)
        if not internal and changes:
            self._join_changes(record)
        return self._format_response(record, format, columns=_S_COLUMNS)

    def session_changes(self, ident, master=False, format=None):
        id, _ = self._id('sessions', ident)
        which = 'master' if master else 'local'
        result = self._get(f'sessions/{id}/changes/{which}/', format='json')
        return self._format_response(result['files'], format=format, columns=_CH_COLUMNS)

    def session_branches(self, ident, format=None):
        id, _ = self._id('sessions', ident)
        # Use master because it's more likely to be a smaller result (no changed files)
        result = self._get(f'sessions/{id}/changes/master/', format='json')
        result = [{'branch': k, 'sha1': v} for k, v in result['branches'].items()]
        return self._format_response(result, format=format, columns=_BR_COLUMNS)

    def session_start(self, ident, editor=None, resource_profile=None, wait=True, format=None):
        id, record = self._id('projects', ident)
        patches = {}
        for key, value in (('editor', editor), ('resource_profile', resource_profile)):
            if value and record.get(key) != value:
                patches[key] = value
        if patches:
            self._patch(f'projects/{id}', json=patches)
        response = self._post(f'projects/{id}/sessions')
        if response.get('error'):
            raise RuntimeError('Error starting project: {}'.format(response['error']['message']))
        if wait:
            response['action'] = self._wait(id, response['action'])
        if response['action'].get('error'):
            raise RuntimeError('Error completing session start: {}'.format(response['action']['message']))
        return self._format_response(response, format=format, columns=_S_COLUMNS)

    def session_stop(self, ident, format=format):
        id, _ = self._id('sessions', ident)
        return self._delete(f'sessions/{id}', format=format)

    def session_restart(self, ident, wait=True, format=None):
        id, record = self._id('sessions', ident)
        self._delete(f'sessions/{id}')
        # Unlike deployments I am not copying over the editor and resource profile
        # settings from the current session. That's because I want to support the use
        # case where the session settings are patched prior to restart
        return self.session_start(record['project_id'], wait=wait, format=format)

    def deployment_list(self, collaborators=True, endpoints=True, internal=False, format=None):
        response = self._get('deployments')
        self._join_projects(response)
        if not internal and collaborators:
            self._join_collaborators('deployments', response)
        if endpoints:
            self._fix_endpoints(response)
        return self._format_response(response, format, _D_COLUMNS)

    def deployment_info(self, ident, collaborators=True, internal=False, format=None, quiet=False):
        id, record = self._id('deployments', ident, quiet=quiet)
        if record:
            self._join_projects(record)
            if collaborators and not internal:
                self._join_collaborators('deployments', record)
            if record.get('url'):
                record['endpoint'] = record['url'].split('/', 3)[2].split('.', 1)[0]
        return self._format_response(record, format, _D_COLUMNS)

    def endpoint_list(self, format=None, internal=False):
        response = self._get('/platform/deploy/api/v1/apps/static-endpoints')
        response = response['data']
        deps = self.deployment_list(internal=True)
        dmap = {drec['endpoint']: drec for drec in deps if drec['endpoint']}
        pnames = {prec['id']: prec['name'] for prec in self.project_list(internal=True)}
        for rec in response:
            drec = dmap.get(rec['id'])
            if drec:
                rec['project_url'] = drec['project_url']
                rec['project_name'], rec['project_id'] = drec['project_name'], drec['project_id']
                rec['name'], rec['deployment_id'] = drec['name'], drec['id']
                rec['owner'] = drec['owner']
            else:
                rec['name'], rec['deployment_id'] = '', ''
                rec['project_id'] = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
                rec['project_name'] = pnames.get(rec['project_id'], '')
        return self._format_response(response, format=format, columns=_E_COLUMNS)

    def endpoint_info(self, ident, internal=False, format=None, quiet=False):
        id, rec = self._id_or_name('endpoint', ident, quiet=quiet)
        return self._format_response(rec, format=format, columns=_E_COLUMNS)

    def deployment_collaborators(self, ident, format=None):
        id, _ = self._id('deployments', ident)
        return self._get(f'deployments/{id}/collaborators', format=format, columns=_C_COLUMNS)

    def deployment_collaborator_list(self, ident, format=None):
        id, _ = self._id('deployments', ident)
        return self._get(f'deployments/{id}/collaborators', format=format, columns=_C_COLUMNS)

    def deployment_collaborator_info(self, ident, userid, internal=False, format=None, quiet=False):
        collabs = self.deployment_collaborator_list(ident)
        for c in collabs:
            if userid == c['id']:
                return self._format_response(c, format=format, columns=_C_COLUMNS)
        if not quiet:
            raise AEException(f'Collaborator not found: {userid}')

    def deployment_collaborator_list_set(self, ident, collabs, format=None):
        id, _ = self._id('deployments', ident)
        result = self._put(f'deployments/{id}/collaborators', json=collabs)
        return self._format_response(result['collaborators'], format=format, columns=_C_COLUMNS)

    def deployment_collaborator_add(self, ident, userid, group=False, format=None):
        id, _ = self._id('deployments', ident)
        collabs = self.deployment_collaborator_list(id)
        ncollabs = len(collabs)
        if not isinstance(userid, tuple):
            userid = userid,
        collabs = [c for c in collabs if c['id'] not in userid]
        if len(collabs) != ncollabs:
            self.deployment_collaborator_list_set(id, collabs)
        collabs.extend({'id': u, 'type': 'group' if group else 'user', 'permission': 'r'} for u in userid)
        return self.deployment_collaborator_list_set(id, collabs, format=format)

    def deployment_collaborator_remove(self, ident, userid, format=None):
        id, _ = self._id('deployments', ident)
        collabs = self.deployment_collaborator_list(id)
        if not isinstance(userid, tuple):
            userid = userid,
        missing = set(userid) - set(c['id'] for c in collabs)
        if missing:
            missing = ', '.join(missing)
            raise AEException(f'Collaborator(s) not found: {missing}')
        collabs = [c for c in collabs if c['id'] not in userid]
        return self.deployment_collaborator_list_set(id, collabs, format=format)

    def deployment_start(self, ident, name=None, endpoint=None, command=None,
                         resource_profile=None, public=False,
                         collaborators=None, wait=True,
                         stop_on_error=False, format=None):
        id, rev, prec, rrec = self._revision(ident)
        data = {'source': rrec['url'],
                'revision': rrec['id'],
                'resource_profile': resource_profile or prec['resource_profile'],
                'command': command or rrec['commands'][0]['id'],
                'public': bool(public),
                'target': 'deploy'}
        if name:
            data['name'] = name
        if endpoint:
            data['static_endpoint'] = endpoint
        response = self._post(f'projects/{id}/deployments', json=data)
        if response.get('error'):
            raise RuntimeError('Error starting deployment: {}'.format(response['error']['message']))
        if collaborators:
            self.deployment_collaborator_list_set(response['id'], collaborators)
        # The _wait method doesn't work here. The action isn't even updated, it seems
        if wait or stop_on_error:
            while response['state'] in ('initial', 'starting'):
                time.sleep(5)
                response = self._get(f'deployments/{response["id"]}')
            if response['state'] != 'started':
                if stop_on_error:
                    self.deployment_stop(response["id"])
                raise RuntimeError(f'Error completing deployment start: {response["status_text"]}')
        response['project_id'] = id
        return self._format_response(response, format=format, columns=_S_COLUMNS)

    def deployment_restart(self, ident, wait=True, stop_on_error=False, format=None):
        id, record = self._id('deployments', ident)
        collab = self.deployment_collaborators(id)
        if record.get('url'):
            endpoint = record['url'].split('/', 3)[2].split('.', 1)[0]
            if id.endswith(endpoint):
                endpoint = None
        else:
            endpoint = None
        self._delete(f'deployments/{id}')
        return self.deployment_start(record['project_id'],
                                     endpoint=endpoint, command=record['command'],
                                     resource_profile=record['resource_profile'], public=record['public'],
                                     collaborators=collab, wait=wait,
                                     stop_on_error=stop_on_error, format=format)

    def deployment_patch(self, ident, **kwargs):
        format = kwargs.pop('format', None)
        id, _ = self._id('deployments', ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            self._patch(f'deployments/{id}', json=data)
        return self.deployment_info(id, format=format)

    def deployment_stop(self, ident, format=None):
        id, _ = self._id('deployments', ident)
        return self._delete(f'deployments/{id}', format=format)

    def deployment_logs(self, ident, which=None, format=None):
        id, _ = self._id('deployments', ident)
        result = self._get(f'deployments/{id}/logs', format='json')
        if which is not None:
            result = result[which]
        return self._format_response(result, format=format)

    def deployment_token(self, ident, which=None, format=None):
        id, _ = self._id('deployments', ident)
        result = self._post(f'deployments/{id}/token', format='json')
        if isinstance(result, dict) and set(result) == {'token'}:
            result = result['token']
        return self._format_response(result, format=format)

    def job_list(self, internal=False, format=None):
        return self._get('jobs', format=format, columns=_J_COLUMNS)

    def job_info(self, ident, internal=False, format=None, quiet=False):
        id, record = self._id('jobs', ident, quiet=quiet)
        return self._format_response(record, format=format, columns=_J_COLUMNS)

    def job_runs(self, ident, format=None):
        id, record = self._id('jobs', ident)
        return self._get(f'jobs/{id}/runs', format=format, columns=_J_COLUMNS)

    def job_run(self, ident, format=None):
        id, _ = self._id('jobs', ident)
        return self._post(f'jobs/{id}/runs', format=format, columns=_J_COLUMNS)

    def job_delete(self, ident, format=None):
        id, _ = self._id('jobs', ident)
        return self._delete(f'jobs/{id}', format=format)

    def job_pause(self, ident, format=None):
        id, _ = self._id('jobs', ident)
        return self._post(f'jobs/{id}/pause', format=format, columns=_J_COLUMNS)

    def job_unpause(self, ident, format=format):
        id, _ = self._id('jobs', ident)
        return self._post(f'jobs/{id}/unpause', format=format, columns=_J_COLUMNS)

    def job_create(self, ident, schedule=None, name=None, command=None,
                   resource_profile=None, variables=None, run=False,
                   wait=False, cleanup=False, make_unique=None,
                   show_run=False, format=None):
        if cleanup and schedule:
            raise ValueError('cannot use cleanup=True with a scheduled job')
        if cleanup and (not run or not wait):
            raise ValueError('must specify run=wait=True with cleanup=True')
        id, rev, prec, rrec = self._revision(ident, keep_latest=True)
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
                'revision': rev or 'latest',
                'name': name}
        if variables:
            data['variables'] = variables
        response = self._post(f'projects/{id}/jobs', json=data)
        if response.get('error'):
            raise RuntimeError('Error starting job: {}'.format(response['error']['message']))
        response['project_id'] = id
        if run:
            run = self._get(f'jobs/{response["id"]}/runs')[-1]
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
        id, jrec = self._id('jobs', ident)
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
        return self._patch(f'jobs/{id}', json=data, format=format, columns=_J_COLUMNS)

    def run_list(self, internal=False, format=None):
        return self._get('runs', format=format, columns=_J_COLUMNS)

    def run_info(self, ident, internal=False, format=None, quiet=False):
        id, record = self._id('runs', ident, quiet=quiet)
        return self._format_response(record, format=format, columns=_J_COLUMNS)

    def run_log(self, ident, format=None):
        id, _ = self._id('runs', ident)
        return self._get(f'runs/{id}/logs')['job']

    def run_stop(self, ident, format=None):
        id, _ = self._id('runs', ident)
        return self._post(f'runs/{id}/stop', format=format, columns=_J_COLUMNS)

    def run_delete(self, ident, format=None):
        id, _ = self._id('runs', ident)
        return self._delete(f'runs/{id}', format=format, columns=_J_COLUMNS)


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
        return self._format_response(users, format=format, columns=_U_COLUMNS)

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
        return self._format_response(response, format, _U_COLUMNS)

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
