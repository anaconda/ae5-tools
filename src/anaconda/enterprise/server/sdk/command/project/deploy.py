from typing import Union

from ...session.admin import AEAdminSession
from ...session.user import AEUserSession
from ..abstract_command import AbstractCommand


class ProjectDeployCommand(AbstractCommand):
    def execute(self, project_id: str, session: Union[AEAdminSession, AEUserSession]) -> None:

        # Get revisions
        response = self._get_records(f"projects/{id}/revisions", filter=filter, project=prec, retry_if_empty=True)

        data = {
            "source": rrec["url"],
            "revision": rrec["name"],
            "resource_profile": resource_profile,
            "command": command,
            "public": bool(public),
            "target": "deploy",
        }
        response = session._api(method="post", endpoint=f"projects/{project_id}/deployments", api_kwargs={"json": data})
