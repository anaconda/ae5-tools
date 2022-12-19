from abc import abstractmethod
from typing import Optional

from ...contracts.dto.base_model import BaseModel


class AEK8SAbstractClient(BaseModel):
    _error: Optional[str] = None

    @abstractmethod
    def _api(self, method: str, path: str, **kwargs):
        """ """

    def error(self):
        return self._error

    def status(self):
        return self._api("get", "").text

    def node_info(self):
        return self._api("get", "nodes").json()

    def pod_info(self, ids):
        result = self._api("post", "pods", json=ids).json()
        result = [result.get(x) for x in ids]
        return result

    def pod_log(self, id, container=None, follow=False):
        follow_s = str(bool(follow)).lower()
        path = f"pod/{id}/log?follow={follow_s}"
        if container is not None:
            path = f"{path}&container={container}"
        response = self._api("get", path, stream=True)
        for chunk in response.iter_content():
            if chunk:
                # TODO: what steam?
                stream.write(chunk.decode("utf-8", errors="replace"))
