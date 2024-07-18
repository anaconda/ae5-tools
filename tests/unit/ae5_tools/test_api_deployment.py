from unittest.mock import MagicMock

from ae5_tools import AEUserSession

base_params: dict = {"hostname": "mock-hostname", "username": "mock-username", "password": "<PASSWORD>", "persist": False}


def test_deployment_restart_with_same_name():
    user_session: AEUserSession = AEUserSession(**base_params)

    mock_drec: dict = {
        "id": "mock-id",
        "project_id": "mock-project-id",
        "revision": "mock-revision",
        "name": "mock-name",
        "command": "mock-command",
        "resource_profile": "mock-resource-profile",
        "public": False,
    }
    user_session._ident_record = MagicMock(return_value=mock_drec)
    user_session.deployment_collaborator_list = MagicMock(return_value={})
    user_session.deployment_stop = MagicMock()
    user_session.deployment_info = MagicMock(side_effect=[Exception("No deployments found matching")])
    user_session.deployment_start = MagicMock()

    request_params: dict = {"ident": "mock-ident", "wait": False, "format": "json"}
    user_session.deployment_restart(**request_params)

    user_session.deployment_start.assert_called_once_with(
        "mock-project-id:mock-revision",
        name="mock-name",
        endpoint=None,
        command="mock-command",
        resource_profile="mock-resource-profile",
        public=False,
        collaborators={},
        wait=False,
        open=False,
        frame=True,
        stop_on_error=False,
        format="json",
        _skip_endpoint_test=True,
    )


def test_deployment_start():
    user_session: AEUserSession = AEUserSession(**base_params)

    request_params: dict = {"ident": "mock-ident", "wait": False, "format": "json"}
