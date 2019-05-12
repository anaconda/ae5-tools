import requests
import time
import io
import re
import pandas as pd
from lxml import html
from os.path import basename
from fnmatch import fnmatch
import getpass
import atexit

from .config import config
from .identifier import Identifier

requests.packages.urllib3.disable_warnings()


def _logout_at_exit(session, url, **args):
    session.get(url, **args)


_P_COLUMNS = ['name', 'owner', 'editor',  'resource_profile',                                    'id', 'project_id', 'created', 'updated', 'url']
_R_COLUMNS = ['name', 'owner', 'commands',                                                       'id', 'project_id', 'created', 'updated', 'url']
_S_COLUMNS = ['name', 'owner',            'resource_profile',                           'state', 'id', 'project_id', 'created', 'updated', 'url']
_D_COLUMNS = ['name', 'owner', 'command', 'resource_profile', 'project_name', 'public', 'state', 'id', 'project_id', 'created', 'updated', 'url']
_J_COLUMNS = ['name', 'owner', 'command', 'resource_profile', 'project_name',           'state', 'id', 'project_id', 'created', 'updated', 'url']
_C_COLUMNS = ['id', 'permission', 'type', 'first name', 'last name', 'email']
_U_COLUMNS = ['username', 'email', 'firstName', 'lastName', 'id']
_A_COLUMNS = ['type', 'status', 'message', 'done', 'owner', 'id', 'description', 'created', 'updated']
_DTYPES = {'created': 'datetime', 'updated': 'datetime',
           'createdTimestamp': 'timestamp/ms', 'notBefore': 'timestamp/s'}


class AESessionBase(object):
    '''Base class for AE5 API interactions.'''

    def __init__(self, prefix, session, dataframe=False):
        '''Base class constructor.

        Args:
            prefix (str): The URL prefix to prepend to all API calls.
                This will include the scheme, the hostname, and any
                base path.
            session (Requests.Session): a valid Requests session with
                any authorization, cookies, and headers preset.
            dataframe (bool, optional, default=False): if True, any
                API call made with a `columns` argument will be
                returned as a dataframe. If False, the raw JSON output
                will be returned instead. 
        '''
        self.prefix = prefix
        self.session = session
        self.dataframe = dataframe

    def _format_kwargs(self, kwargs):
        dataframe = kwargs.pop('dataframe', None)
        format = kwargs.pop('format', None)
        if dataframe is not None:
            if format is None:
                format = 'dataframe' if dataframe else 'json'
            elif (format == 'dataframe') != dataframe:
                raise RuntimeError('Conflicting "format" and "dataframe" specifications')
        return format, kwargs.pop('columns', None)

    def _format_response(self, response, format, columns):
        if format == 'response':
            return response
        if format == 'blob':
            return response.content
        if isinstance(response, requests.models.Response):
            response = response.json()
        if format is None and columns:
            format = 'dataframe' if self.dataframe else 'json'
        if format == 'json':
            return response
        if isinstance(response, dict):
            is_series = True
        elif isinstance(response, list) and all(isinstance(x, dict) for x in response):
            is_series = False
        else:
            raise RuntimeError('Not a dataframe-compatible output')
        import pandas as pd
        df = pd.DataFrame([response] if is_series else response)
        if len(df) == 0 and columns:
            df = pd.DataFrame(columns=columns)
        for col, dtype in _DTYPES.items():
            if col in df:
                if dtype == 'datetime':
                    df[col] = pd.to_datetime(df[col])
                elif dtype.startswith('timestamp'):
                    df[col] = pd.to_datetime(df[col], unit=dtype.rsplit('/', 1)[-1])
                else:
                    df[col] = df[col].astype(dtype)
        if columns:
            cols = ([c for c in columns if c in df.columns] +
                    [c for c in df.columns if c not in columns])
            if cols:
                df = df[cols]
        if is_series:
            df = df.iloc[0]
            df.name = None
        return df

    def _api(self, method, endpoint, **kwargs):
        pass_errors = kwargs.pop('pass_errors', False)
        fmt, cols = self._format_kwargs(kwargs)
        if endpoint.startswith('/'):
            url = '/'.join(self.prefix.split('/', 3)[:3]) + endpoint
        else:
            url = self.prefix + endpoint
        kwargs.update((('verify', False), ('allow_redirects', True)))
        response = getattr(self.session, method)(url, **kwargs)
        print(response.url)
        if 400 <= response.status_code and not pass_errors:
            msg = (f'Unexpected response: {response.status_code} {response.reason}\n'
                   f'  {method} {url}')
            raise ValueError(msg)
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
    def __init__(self, hostname, username, password, dataframe=False):
        if not hostname or not username:
            raise ValueError('Must supply hostname and username')
        if isinstance(password, requests.Session):
            session = password
        else:
            session = config.session(hostname, username, password)
        prefix = f'https://{hostname}/api/v2/'
        super(AEUserSession, self).__init__(prefix, session, dataframe)

    def _id(self, type, ident, ignore_revision=False):
        if isinstance(ident, str):
            ident = Identifier.from_string(ident)
        if not ident.id or (ident.owner or ident.name):
            matches = []
            owner, name, id = ident.owner or '*', ident.name or '*', ident.id or '*'
            for record in getattr(self, type[:-1] + '_list')(format='json'):
                if (fnmatch(record['name'], name) and fnmatch(record['owner'], owner)
                    and fnmatch(record['id'], id)):
                   matches.append(record)
            if len(matches) != 1:
                pfx = 'Multiple' if len(matches) else 'No'
                msg = f'{pfx} {type} found matching {owner}/{name}/{id}'
                if matches:
                    matches = [str(Identifier.from_record(r, ignore_revision)) for r in matches]
                    msg += ':\n  - ' + '\n  - '.join(matches)
                raise ValueError(msg)
            return matches[0]['id']
        return ident.id

    def _revision(self, ident):
        if isinstance(ident, str):
            ident = Identifier.from_string(ident)
        id = self._id('projects', ident)
        revisions = self._get(f'projects/{id}/revisions', format='json')
        if not ident.revision or ident.revision == 'latest':
            matches = [revisions[0]]
        else:
            matches = []
            for response in revisions:
                if fnmatch(response['name'], ident.revision):
                    matches.append(response)
        if len(matches) != 1:
            pfx = 'Multiple' if len(matches) else 'No'
            msg = f'{pfx} revisions found matching {ident.revision}'
            if matches:
                msg += ':\n  - ' + '\n  - '.join(matches)
            raise ValueError(msg)
        rev = matches[0]['name']
        return id, rev

    def project_list(self, format=None):
        return self._get('projects', format=format, columns=_P_COLUMNS)

    def project_info(self, ident, format=None):
        id = self._id('projects', ident)
        return self._get(f'projects/{id}', format=format, columns=_P_COLUMNS)

    def project_collaborators(self, ident, format=None):
        id = self._id('projects', ident)
        return self._get(f'projects/{id}/collaborators', columns=_C_COLUMNS)

    def project_activity(self, ident, limit=0, latest=False, format=None):
        id = self._id('projects', ident)
        limit = 1 if latest else (999999 if limit <= 0 else limit)
        params = {'sort':'-updated', 'page[size]': limit}
        response = self._get(f'projects/{id}/activity', params=params, format='json')['data']
        if latest:
            response = response[0]
        return self._format_response(response, format=format, columns=_A_COLUMNS)

    def revision_list(self, ident, format=None):
        id = self._id('projects', ident)
        response = self._get(f'projects/{id}/revisions', format='json')
        for rec in response:
            rec['project_id'] = 'a0-' + rec['url'].rsplit('/', 3)[-3]
        return self._format_response(response, format=format, columns=_R_COLUMNS)

    def revision_info(self, ident, format=None):
        id, rev = self._revision(ident)
        rec = self._get(f'projects/{id}/revisions/{rev}', format='json')
        rec['project_id'] = 'a0-' + rec['url'].rsplit('/', 3)[-3]
        return self._format_response(rec, format=format, columns=_R_COLUMNS)

    def project_download(self, ident, filename=None):
        id, rev = self._revision(ident)
        response = self._get(f'projects/{id}/revisions/{rev}/archive', format='blob')
        if filename is None:
            return response
        with open(filename, 'wb') as fp:
            fp.write(response)

    def project_delete(self, ident):
        id = self._id('projects', ident)
        self._delete(f'projects/{id}', format='response')

    def project_upload(self, project_archive, name, wait=True):
        if name is None:
            if type(project_archive) == bytes:
                raise RuntimeError('Project name must be supplied for binary input')
            name = basename(project_archive).split('.', 1)[0]
        try:
            if type(project_archive) == bytes:
                f = io.BytesIO(project_archive)
            else:
                f = open(project_archive, 'rb')
            data = {'name': name}
            response = self._post('projects/upload', files={'project_file': f},
                                  data={'name': name}, format='json')
        finally:
            f.close()
        if response.get('error'):
            raise RuntimeError('Error uploading project: {}'.format(response['error']['message']))
        while True:
            status = self._get(f'projects/{response["id"]}/activity', format='json')['data'][-1]
            if not wait or status['error'] or status['status'] == 'created':
                break
            time.sleep(5)
        if status['error']:
            raise RuntimeError('Error processing upload: {}'.format(status['message']))
        return status

    def _join_projects(self, response, nameprefix=None):
        if isinstance(response, dict):
            pid = 'a0-' + response['project_url'].rsplit('/', 1)[-1]
            if nameprefix:
                project = self._get(f'projects/{pid}', format='json')
                response[f'{nameprefix}_name'] = response['name']
                response['name'] = project['name']
            response['project_id'] = pid
        else:
            if nameprefix:
                pnames = {x['id']: x['name'] for x in self.project_list(format='json')}
            for rec in response:
                pid = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
                if nameprefix:
                    rec[f'{nameprefix}_name'] = rec['name']
                    rec['name'] = pnames[pid]
                rec['project_id'] = pid
    
    def session_list(self, format=None):
        response = self._get('sessions', format='json')
        self._join_projects(response, 'session')
        return self._format_response(response, format, _S_COLUMNS)

    def session_info(self, ident, format=None):
        id = self._id('sessions', ident)
        response = self._get(f'sessions/{id}', format='json')
        self._join_projects(response, 'session')
        return self._format_response(response, format, columns=_S_COLUMNS)

    def _wait(self, id, status):
        index = 0
        while not status['done'] and not status['error']:
            time.sleep(5)
            params = {'sort':'-updated', 'page[size]': index + 1}
            activity = self._get(f'projects/{id}/activity', params=params, format='json')
            try:
                status = next(s for s in activity['data'] if s['id'] == status['id'])
            except StopIteration:
                index = index + 1
        return status

    def session_start(self, ident, wait=True, format=None):
        id = self._id('projects', ident)
        response = self._post(f'projects/{id}/sessions', format='json')
        if response.get('error'):
            raise RuntimeError('Error starting project: {}'.format(response['error']['message']))
        if wait:
            response['action'] = self._wait(id, response['action'])
        response['project_id'] = id
        return self._format_response(response, format=format, columns=_S_COLUMNS)

    def session_stop(self, ident):
        id = self._id('sessions', ident)
        self._delete(f'sessions/{id}', format='response')

    def deployment_list(self, format=None):
        response = self._get('deployments', format='json')
        self._join_projects(response)
        return self._format_response(response, format, _D_COLUMNS)

    def deployment_info(self, ident, format=None):
        id = self._id('deployments', ident, ignore_revision=True)
        response = self._get(f'deployments/{id}', format='json')
        self._join_projects(response)
        return self._format_response(response, format, _D_COLUMNS)

    def deployment_collaborators(self, ident, format=None):
        id = self._id('deployments', ident)
        return self._get(f'deployments/{id}/collaborators', format=format, columns=_C_COLUMNS)

    def deployment_stop(self, ident):
        id = self._id('deployments', ident)
        self._delete(f'deployments/{id}', format='response')

    def job_list(self, format=None):
        return self._get('jobs', format=format, columns=_J_COLUMNS)

    def job_info(self, ident, format=None):
        id = self._id('jobs', ident)
        response = self._get(f'jobs/{id}', format=format, columns=_J_COLUMNS)
        return response

    def job_stop(self, ident):
        id = self._id('jobs', ident)
        self._delete(f'jobs/{id}', format='response')


class AEAdminSession(AESessionBase):
    def __init__(self, hostname, username, password=None, dataframe=False):
        if not hostname or not username:
            raise ValueError('Must supply hostname and username')
        session = config.admin_session(hostname, username, password)
        prefix = f'https://{hostname}/auth/admin/realms/AnacondaPlatform/'
        super(AEAdmin, self).__init__(prefix, session, dataframe)
        self.hostname = hostname

    def user_list(self, format=None):
        return self._get(f'users', format=format, columns=_U_COLUMNS)

    def user_info(self, user_or_id, format=None):
        if re.match(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', user_or_id):
            response = [self._get(f'users/{user_or_id}', format='json')]
        else:
            response = self._get(f'users?username={user_or_id}', format='json')
        if len(response) == 0:
            raise ValueError(f'Could not find user {user_or_id}')
        return self._format_response(response[0], format, _U_COLUMNS)

    def impersonate(self, user_or_id):
        record = self.user_info(user_or_id, format='json')
        try:
            self._post(f'users/{record["id"]}/impersonation', format='response')
            params = {'client_id': 'anaconda-platform', 'scope': 'openid', 'response_type': 'code',
                      'redirect_uri': f'https://{self.hostname}/login'}
            self._get('/auth/realms/AnacondaPlatform/protocol/openid-connect/auth', params=params, format='response')
            nsession = requests.Session()
            nsession.cookies, self.session.cookies = self.session.cookies, nsession.cookies
            nsession.headers = self.session.headers.copy()
            del nsession.headers['Authorization']
            return AEUserSession(self.hostname, record["username"], nsession)
        finally:
            self.session.cookies.clear()

