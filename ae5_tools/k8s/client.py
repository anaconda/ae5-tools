import asyncio
import requests
import sys
import io

from .transformer import AE5K8STransformer
from .ssh import tunneled_k8s_url


class AE5K8SLocalClient(object):
    def __init__(self, hostname, username):
        url = tunneled_k8s_url(hostname, username)
        self.xfrm = AE5K8STransformer(url)

    def _run(self, request):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(request)

    def healthy(self):
        return True

    def node_info(self):
        return self._run(self.xfrm.node_info())

    def pod_info(self, ids):
        return self._run(self.xfrm.pod_info(ids))

    def pod_log(self, id, container=None, follow=False):
        self._run(self.xfrm.pod_log(id, container, follow))


class AE5K8SRemoteClient(object):
    def __init__(self, session):
        self._session = session

    def _get(self, path, **kwargs):
        return self._session._get(path, subdomain='k8s', **kwargs)

    def healthy(self):
        try:
            return self._get('__status__', format='text') == 'Alive and kicking'
        except Exception:
            return False

    def node_info(self):
        return self._get('nodes', format='json')

    def pod_info(self, ids):
        if isinstance(ids, str):
            path = f'pod/{ids}'
        else:
            path = f'pods?' + '&'.join('id=' + x for x in ids)
        return self._get(path, format='json')

    def pod_log(self, id, container=None, follow=False):
        follow_s = str(bool(follow)).lower()
        path = f'pod/{id}/log?follow={follow_s}'
        if container is not None:
            path = f'{path}&container={container}'
        response = self._get(path, format='response', stream=True)
        for chunk in response.iter_content():
            if chunk:
                stream.write(chunk.decode('utf-8', errors='replace'))
