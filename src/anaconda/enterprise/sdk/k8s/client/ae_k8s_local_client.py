import sys
from typing import Any

import requests

from ..server.constants import K8S_ENDPOINT_PORT
from ..ssh import launch_background, tunneled_k8s_url
from .ae_k8s_abstract_client import AEK8SAbstractClient


class AEK8SLocalClient(AEK8SAbstractClient):
    hostname: str
    username: str

    def __init__(self, hostname: str, username: str, **data: Any):
        super().__init__(**data)

        self._ssh = self._server = None
        try:
            self._ssh, ssh_url = tunneled_k8s_url(hostname, username)
        except RuntimeError as exc:
            self._error = str(exc)
            return
        cmd = ["python", "-u", "-m", "ae5_tools.k8s.server", ssh_url]
        try:
            self._server = launch_background(cmd, "======== Running on", "start server")
            self._error = None
        except RuntimeError as exc:
            self._error = str(exc)

    def disconnect(self):
        if self._server is not None and self._server.returncode is None:
            self._server.terminate()
            self._server.communicate()
            self._server = None
        if self._ssh is not None and self._ssh.returncode is None:
            self._ssh.terminate()
            self._ssh.communicate()
            self._ssh = None

    def __del__(self):
        if sys.meta_path is not None:
            self.disconnect()

    def _api(self, method, path, **kwargs):
        return requests.request(method, f"http://localhost:{K8S_ENDPOINT_PORT}/{path}", **kwargs)
