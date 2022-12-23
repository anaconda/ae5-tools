from os.path import basename
from typing import Any, Union

import urllib3

from ...ae.session.admin import AEAdminSession
from ...ae.session.user import AEUserSession
from ...contract.dto.request.project_create import ProjectCreateRequest
from ..abstract_command import AbstractCommand


class ProjectCreateCommand(AbstractCommand):
    def execute(self, request: ProjectCreateRequest, session: Union[AEAdminSession, AEUserSession]) -> Any:
        if not request.name:
            parts = urllib3.util.parse_url(request.source)
            request.name = basename(parts.path).split(".", 1)[0]

        # TODO: check the param serialization here when looking into the underlying calls....
        response = session._post_record("projects", api_kwargs={"json": request.dict()})
        if response.get("error"):
            raise RuntimeError("Error creating project: {}".format(response["error"]["message"]))
        if response["action"]["error"]:
            raise RuntimeError("Error processing creation: {}".format(response["action"]["message"]))
        return response
