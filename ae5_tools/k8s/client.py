import sys
import requests
import subprocess

from .ssh import launch_background, tunneled_k8s_url


class AE5K8SClient(object):
    def error(self):
        return self._error

    def status(self):
        return self._api('get', '').text

    def node_info(self):
        return self._api('get', 'nodes').json()

    def pod_info(self, ids):
        result = self._api('post', 'pods', json=ids).json()
        result = [result.get(x) for x in ids]
        return result

    def pod_log(self, id, container=None, follow=False):
        follow_s = str(bool(follow)).lower()
        path = f'pod/{id}/log?follow={follow_s}'
        if container is not None:
            path = f'{path}&container={container}'
        response = self._api('get', path, stream=True)
        for chunk in response.iter_content():
            if chunk:
                stream.write(chunk.decode('utf-8', errors='replace'))


class AE5K8SLocalClient(AE5K8SClient):
    def __init__(self, hostname, username):
        self._ssh = self._server = None
        try:
            self._ssh, ssh_url = tunneled_k8s_url(hostname, username)
        except RuntimeError as exc:
            self._error = str(exc)
            return
        cmd = ['python', '-u', '-m', 'ae5_tools.k8s.server', ssh_url]
        try:
            self._server = launch_background(cmd, '======== Running on', 'start server')
            self._error = None
        except RuntimeError as exc:
            self._error = str(exc)

    def __del__(self):
        if sys.meta_path is not None:
            if self._server is not None and self._server.returncode is None:
                self._server.terminate()
                self._server.communicate()
            if self._ssh is not None and self._ssh.returncode is None:
                self._ssh.terminate()
                self._ssh.communicate()

    def _api(self, method, path, **kwargs):
        return getattr(requests, method)(f'http://localhost:8086/{path}', **kwargs)


class AE5K8SRemoteClient(AE5K8SClient):
    def __init__(self, session, subdomain):
        self._session = session
        self._subdomain = subdomain
        try:
            session._head('/_errors/404.html', format='response')
            response = session._get('', subdomain=subdomain, format='text')
            if response == 'Alive and kicking':
                self._error = None
            else:
                self._error = f'Unexpected response at endpoint {subdomain}'
        except RuntimeError as exc:
            self._error = f'No deployment found at endpoint {subdomain}'

    def _api(self, method, path, **kwargs):
        return self._session._api(method, path, subdomain=self._subdomain,
                                  format='response', **kwargs)
