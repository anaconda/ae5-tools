import requests
import time
import io
from lxml import html
from os.path import basename
import getpass

requests.packages.urllib3.disable_warnings()


class AECluster(object):
    request_args = dict(verify=False, allow_redirects=True)

    def __init__(self, hostname, username, password, dataframe=False):
        self.hostname = hostname
        self._df_format = 'dataframe' if dataframe else 'json'
        self.session = requests.Session()
        self.username = username

        url = f'https://{self.hostname}/auth/realms/AnacondaPlatform/protocol/openid-connect/auth?client_id=anaconda-platform&scope=openid+email&response_type=code&redirect_uri=https%3A%2F%2F{self.hostname}%2Flogin'
        r = self.session.get(url, **self.request_args)
        tree = html.fromstring(r.text)
        form = tree.xpath("//form[@id='kc-form-login']")
        login_url = form[0].action

        data = {'username': username, 'password': password}
        r = self.session.post(login_url, data=data, **self.request_args)

        headers = {'x-xsrftoken': r.cookies['_xsrf']}
        self.session_args = {'headers':headers, 'cookies':r.cookies}

    def __del__(self):
        self.session.get(f'https://{self.hostname}/logout', **self.session_args, **self.request_args)

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
        if fmt in ('json', 'dataframe'):
            response = response.json() if isinstance(response, requests.models.Response) else response
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
            if kwargs['dtypes']:
                for col, dtype in kwargs['dtypes'].items():
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
        if fmt == 'response':
            return response
        if fmt == 'text':
            return response.text
        if isinstance(response, requests.models.Response):
            response = response.content
        return response

    def _api(self, method, endpoint, **kwargs):
        fmt_args = self._format_kwargs(kwargs)
        url = f"https://{self.hostname}/api/v2/{endpoint}"
        print('{} {}'.format(method.upper(), url))
        response = getattr(self.session, method)(url, **kwargs, **self.session_args, **self.request_args)
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

    def projects(self, **kwargs):
        columns = ['name', 'owner', 'editor', 'resource_profile', 'id', 'created', 'updated', 'project_create_status']
        dtypes = {'created': 'datetime', 'updated': 'datetime'}
        return self._get('projects', columns=columns, dtypes=dtypes, **kwargs)

    def sessions(self, **kwargs):
        columns = ['name', 'owner', 'resource_profile', 'state', 'id', 'created', 'updated']
        dtypes = {'created': 'datetime', 'updated': 'datetime'}
        fmt_args = self._format_kwargs(kwargs, columns=columns, dtypes=dtypes)
        resolve = kwargs.pop('simple_names', True)
        response = self._get('sessions', format='json', **kwargs)
        if resolve:
            pnames = {x['url']: x['name'] for x in self.projects(format='json')}
            for rec in response:
                rec['session_name'] = rec['name']
                rec['name'] = pnames[rec['project_url']]
        return self._format_response(response, **fmt_args)

    def deployments(self, **kwargs):
        columns = ['name', 'project_name', 'owner', 'command', 'resource_profile', 'id', 'created', 'updated', 'state']
        dtypes = {'created': 'datetime', 'updated': 'datetime'}
        return self._get('deployments', columns=columns, dtypes=dtypes)

    def jobs(self, **kwargs):
        columns = ['name', 'project_name', 'owner', 'command', 'resource_profile', 'id', 'created', 'updated', 'state']
        dtypes = {'created': 'datetime', 'updated': 'datetime'}
        return self._get('jobs', columns=columns, dtypes=dtypes)

    def project(self, *args, id=None, name=None, owner=None):
        if id:
            if owner is not None or name is not None:
                raise RuntimeError('Must specify id or owner/name, but not both')
            response = self._get(f'projects/{id}', format='response')
            if response.status_code == 404:
                raise KeyError('Project id not found: {}'.format(id))
            return response.json()
        if name is None:
            raise RuntimeError('Must specify id or owner/name')
        if owner is None:
            owner = self.username
        for project in self.projects(format='json'):
            if project['name'] == name and project['owner'] == owner:
                return project
        raise KeyError('Project owner/name not found: {}/{}'.format(owner, name))

    def project_revisions(self, id=None, name=None, owner=None, **kwargs):
        id = self.project(id=id, name=name, owner=owner)['id']
        columns = ['name', 'owner', 'commands', 'id', 'created', 'updated', 'url']
        dtypes = {'created': 'datetime', 'updated': 'datetime'}
        return self._get(f'projects/{id}/revisions', columns=columns, dtypes=dtypes, **kwargs)

    def project_revision(self, id=None, owner=None, name=None, revision=None, keep_id=False):
        id = self.project(id=id, name=name, owner=owner)['id']
        if id and revision is not None and revision != 'latest':
            response = self._get(f'projects/{id}/revisions/{revision}', format='response')
            if response.status_code == 404:
                 raise KeyError('Revision not found: {}'.format(revision))
            response = response.json()
        else:
            columns = ['name', 'owner', 'created', 'updated']
            dtypes = {'created': 'datetime', 'updated': 'datetime'}
            revisions = self._get(f'projects/{id}/revisions', format='json')
            if revision is None or revision == 'latest':
                response = revisions[0]
            else:
                for response in revisions:
                    if response['name'] == revision:
                        break
                else:
                    raise KeyError('Project revision not found: {}'.format(revision))
        if keep_id:
            response['project_id'] = id
        return response

    def project_download(self, id=None, owner=None, name=None, revision=None, filename=None):
        rev = self.project_revision(id=id, owner=owner, name=name, revision=revision, keep_id=True)
        response = self._get(f'projects/{rev["project_id"]}/revisions/{rev["id"]}/archive', format='blob')
        if filename is None:
            return response
        with open(filename, 'wb') as fp:
            fp.write(response)

    def project_delete(self, id=None, owner=None, name=None):
        id = self.project(id=id, name=name, owner=owner)['id']
        r = self._delete(f'projects/{id}', format='response')
        if r.status_code != 204:
            raise RuntimeError(f'Unexpected response deleting project: {r.status_code} {r.reason}')

    def project_upload(self, project_archive, name=None, owner=None, wait=True):
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
            if owner is not None:
                data['owner'] = owner
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

