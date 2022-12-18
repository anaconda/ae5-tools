from typing import Any

from .ae_k8s_abstract_client import AEK8SAbstractClient


class AEK8SRemoteClient(AEK8SAbstractClient):
    session: Any
    subdomain: Any

    def __init__(self, session: Any, subdomain: Any, **data: Any):
        super().__init__(**data)

        try:
            session._get("projects/actions", params={"q": "create_action"})
        except Exception as exc:
            self._error = f"Issue establishing session: {exc}"
            return
        try:
            response = session._get("", subdomain=subdomain, format="text")
            if response == "Alive and kicking":
                self._error = None
            else:
                self._error = f"Unexpected response at endpoint {subdomain}"
        except RuntimeError:
            self._error = f"No deployment found at endpoint {subdomain}"

    def _api(self, method, path, **kwargs):
        return self.session._api(method, path, subdomain=self.subdomain, format="response", **kwargs)
