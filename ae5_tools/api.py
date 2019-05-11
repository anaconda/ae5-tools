import requests
import time
import io
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


_P_COLUMNS = ['name', 'owner', 'editor',  'resource_profile',                                    'id',               'created', 'updated', 'url']
_R_COLUMNS = ['name', 'owner', 'commands',                                                       'id', 'project_id', 'created', 'updated', 'url']
_S_COLUMNS = ['name', 'owner',            'resource_profile',                           'state', 'id', 'project_id', 'created', 'updated', 'url']
_D_COLUMNS = ['name', 'owner', 'command', 'resource_profile', 'project_name', 'public', 'state', 'id', 'project_id', 'created', 'updated', 'url']
_J_COLUMNS = ['name', 'owner', 'command', 'resource_profile', 'project_name',           'state', 'id', 'project_id', 'created', 'updated', 'url']
_C_COLUMNS = ['id', 'permission', 'type', 'first name', 'last name', 'email']
_DTYPES = {'created': 'datetime', 'updated': 'datetime'}


class AECluster(object):
    request_args = dict(verify=False, allow_redirects=True)

    def __init__(self, hostname, username, password, dataframe=False):
        if not hostname or not username:
            raise ValueError('Must supply hostname and username')
        self.hostname = hostname
        self.username = username
        self._df_format = 'dataframe' if dataframe else 'json'
        self._session = config.session(hostname, username, password)

    def _format_kwargs(self, kwargs, **kwargs2):
        kwargs.update(kwargs2)
        can_dataframe = 'columns' in kwargs or 'dtypes' in kwargs
        dataframe = kwargs.pop('dataframe', None)
        format = kwargs.pop('format', None)
        if format is None:
            if dataframe is None:
                format = self._df_format if can_dataframe else 'blob'
            else:
                format = 'dataframe' if dataframe else 'json'
        elif dataframe is not None and bool(dataframe) != (format == 'dataframe'):
            raise RuntimeError('Conflicting "format" and "dataframe" specifications')
        format_kwargs = {'format': format,
                         'columns': kwargs.pop('columns', None),
                         'dtypes': kwargs.pop('dtypes', None)}
        return format_kwargs

    def _format_response(self, response, **kwargs):
        fmt = kwargs['format']
        if isinstance(response, requests.models.Response):
            response = response.json()
        if fmt == 'json':
            return response
        if isinstance(response, dict):
            is_series = True
        elif isinstance(response, list) and all(isinstance(x, dict) for x in response):
            is_series = False
        else:
            raise RuntimeError('Not a dataframe-compatible output')
        import pandas as pd
        df = pd.DataFrame([response] if is_series else response)
        if len(df) == 0 and kwargs.get('columns'):
            df = pd.DataFrame(columns=kwargs['columns'])
        for col, dtype in _DTYPES.items():
            if col in df:
                if dtype == 'datetime':
                    df[col] = pd.to_datetime(df[col])
                else:
                    df[col] = df[col].astype(dtype)
        if kwargs['columns']:
            cols = ([c for c in kwargs['columns'] if c in df.columns] +
                    [c for c in df.columns if c not in kwargs['columns']])
            if cols:
                df = df[cols]
        if is_series:
            df = df.iloc[0]
            df.name = None
        return df

    def _api(self, method, endpoint, **kwargs):
        prefix = kwargs.pop('prefix', 'api/v2')
        fmt_args = self._format_kwargs(kwargs)
        url = f"https://{self.hostname}/{prefix}/{endpoint}"
        kwargs.update(self.request_args)
        response = getattr(self._session, method)(url, **kwargs)
        if 400 <= response.status_code:
            msg = (f'Unexpected response: {response.status_code} {response.reason}\n'
                   f'  {method} {url}')
            raise ValueError(msg)
        return self._format_response(response, **fmt_args)

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

    def project_list(self, **kwargs):
        return self._get('projects', columns=_P_COLUMNS, **kwargs)

    def project_info(self, ident, **kwargs):
        id = self._id('projects', ident)
        return self._get(f'projects/{id}', columns=_P_COLUMNS, **kwargs)

    def project_collaborators(self, ident, **kwargs):
        id = self._id('projects', ident)
        return self._get(f'projects/{id}/collaborators', columns=_C_COLUMNS, **kwargs)

    def revision_list(self, ident, **kwargs):
        id = self._id('projects', ident)
        response = self._get(f'projects/{id}/revisions', format='json')
        for rec in response:
            rec['project_id'] = 'a0-' + rec['url'].rsplit('/', 3)[-3]
        return self._format_response(response, columns=_R_COLUMNS, **kwargs)

    def revision_info(self, ident, **kwargs):
        id, rev = self._revision(ident)
        rec = self._get(f'projects/{id}/revisions/{rev}', format='json')
        rec['project_id'] = 'a0-' + rec['url'].rsplit('/', 3)[-3]
        return self._format_response(rec, columns=_R_COLUMNS, **kwargs)

    def project_download(self, ident, filename=None):
        id, rev = self._revision(ident)
        response = self._get(f'projects/{id}/revisions/{rev}/archive', format='blob')
        if filename is None:
            return response
        with open(filename, 'wb') as fp:
            fp.write(response)

    def project_delete(self, ident):
        id = self._id('projects', ident)
        self._delete(f'projects/{id}')

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

    def _join_projects(self, response, name_prefix=None):
        if isinstance(response, dict):
            pid = 'a0-' + response['project_url'].rsplit('/', 1)[-1]
            if name_prefix:
                project = self._get(f'projects/{pid}', format='json')
                response[f'{name_prefix}_name'] = response['name']
                response['name'] = project['name']
            response['project_id'] = pid
        else:
            if name_prefix:
                pnames = {x['id']: x['name'] for x in self.project_list(format='json')}
            for rec in response:
                pid = 'a0-' + rec['project_url'].rsplit('/', 1)[-1]
                if name_prefix:
                    rec[f'{name_prefix}_name'] = rec['name']
                    rec['name'] = pnames[pid]
                rec['project_id'] = pid
    
    def session_list(self, **kwargs):
        response = self._get('sessions', format='json')
        self._join_projects(response, 'session')
        return self._format_response(response, columns=_S_COLUMNS, **kwargs)

    def session_info(self, ident, **kwargs):
        id = self._id('sessions', ident)
        response = self._get(f'sessions/{id}', format='json')
        self._join_projects(response, 'session')
        return self._format_response(response, columns=_S_COLUMNS, **kwargs)

    def session_start(self, ident, editor=None, wait=True):
        project = self._id('projects', ident)
        data = {'editor': editor or project['editor']}
        response = self._post(f'projects/{project["id"]}/sessions', data=data, format='json')
        if response.get('error'):
            raise RuntimeError('Error starting project: {}'.format(response['error']['message']))
        while True:
            status = self._get(f'projects/{response["id"]}/activity', format='json')['data'][-1]
            if not wait or status['error'] or status['status'] == 'created':
                break
            time.sleep(5)
        if status['error']:
            raise RuntimeError('Error processing upload: {}'.format(status['message']))
        return status

    def session_stop(self, ident):
        id = self._id('sessions', ident)
        self._delete(f'sessions/{id}')

    def deployment_list(self, **kwargs):
        response = self._get('deployments', format='json')
        self._join_projects(response)
        return self._format_response(response, columns=_D_COLUMNS, **kwargs)

    def deployment_info(self, ident, **kwargs):
        id = self._id('deployments', ident, ignore_revision=True)
        response = self._get(f'deployments/{id}', format='json')
        self._join_projects(response)
        return self._format_response(response, columns=_D_COLUMNS, **kwargs)

    def deployment_collaborators(self, ident, **kwargs):
        id = self._id('deployments', ident)
        return self._get(f'deployments/{id}/collaborators', columns=_C_COLUMNS, **kwargs)

    def deployment_stop(self, ident):
        id = self._id('deployments', ident)
        self._delete(f'deployments/{id}')

    def job_list(self, **kwargs):
        return self._get('jobs', columns=_J_COLUMNS, **kwargs)

    def job_info(self, ident, **kwargs):
        id = self._id('jobs', ident)
        response = self._get(f'jobs/{id}', columns=_J_COLUMNS, **kwargs)
        return response

    def job_stop(self, ident):
        id = self._id('jobs', ident)
        self._delete(f'jobs/{id}')

