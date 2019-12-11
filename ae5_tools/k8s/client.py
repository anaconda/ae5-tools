import asyncio
import requests
import sys
import io

from .transformer import AE5K8STransformer
from .ssh import tunneled_k8s_url


class AE5K8SLocalClient(object):
    def __init__(self, hostname, username):
        self._error = self.xfrm = None
        try:
            url = tunneled_k8s_url(hostname, username)
            self.xfrm = AE5K8STransformer(url)
        except RuntimeError as exc:
            self._error = str(exc)

    def _run(self, request):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(request)

    def error(self):
        return self._error

    def status(self):
        return 'Alive and kicking'

    def node_info(self):
        return self._run(self.xfrm.node_info())

    def pod_info(self, ids):
        return self._run(self.xfrm.pod_info(ids))

    def pod_log(self, id, container=None, follow=False):
        self._run(self.xfrm.pod_log(id, container, follow))


class AE5K8SRemoteClient(object):
    def __init__(self, session, subdomain):
        self._error = None
        try: 
            session._head('/_errors/404.html', subdomain=subdomain, format='response')
            response = session._get('/', subdomain=subdomain, format='text')
            if response == 'Alive and kicking':
                self._session = session
                self._subdomain = subdomain
            else:
                self._error = f'Unexpected response at endpoint {subdomain}'
        except RuntimeError:
            self._error = f'No deployment found at endpoint {subdomain}'

    def _get(self, path, **kwargs):
        return self._session._get(path, subdomain=self._subdomain, **kwargs)

    def error(self):
        return self._error

    def status(self):
        return self._get('/', format='text')

    def node_info(self):
        return self._get('nodes', format='json')

    def pod_info(self, ids):
        path = f'pods?' + '&'.join('id=' + x for x in ids)
        result = self._get(path, format='json')
        result = [result[x] for x in ids]
        return result

    def pod_log(self, id, container=None, follow=False):
        follow_s = str(bool(follow)).lower()
        path = f'pod/{id}/log?follow={follow_s}'
        if container is not None:
            path = f'{path}&container={container}'
        response = self._get(path, format='response', stream=True)
        for chunk in response.iter_content():
            if chunk:
                stream.write(chunk.decode('utf-8', errors='replace'))
