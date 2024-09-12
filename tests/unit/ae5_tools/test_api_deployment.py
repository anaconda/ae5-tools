from unittest.mock import MagicMock

import pytest

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


@pytest.mark.skip(reason="work in progress")
def test_deployment_start():
    user_session: AEUserSession = AEUserSession(**base_params)

    request_params: dict = {"ident": "mock-ident", "wait": False, "format": "json"}


def mock_ident(record_type, ident):
    if record_type == "deployment":
        mock_ident = ident
    elif record_type == "project":
        mock_ident = {"id": ident["id"], "project_url": ident["url"] if ident["_record_type"] == "project" else None, "_record_type": "project"}
        if "_revision" in ident:
            mock_ident["_revision"] = ident["_revision"]
        print(f"\n\n{ident=}\n{mock_ident=}")
    return mock_ident


test_data = [
    ({"public": False}, "latest"),  # latest is 0.1.1 - patch is not called
    ({"public": False}, "None"),  # latest is 0.1.1 - patch is not called
    ({"public": False, "resource_profile": "default"}, "0.1.1"),  # patch is not called
    ({"public": True}, "0.1.0"),  # patch is called once
    ({"resource_profile": "large", "public": True}, "0.1.0"),  # patch is called once
]


@pytest.mark.parametrize("kwargs_attr, revision", test_data)
def test_deployment_patch(kwargs_attr, revision):
    user_session: AEUserSession = AEUserSession(**base_params)
    # Assume latest is 0.1.1
    latest = "0.1.1"
    mock_ident: dict = {
        "id": "mock-id",
        "project_id": "mock-project-id",
        "revision": latest,
        "resource_profile": "default",
        "public": False,
        "project_url": "http://anaconda-enterprise-ap-storage/projects/mock-project",
        "_record_type": "deployment",
    }
    # revision is input to CLI separated by colon, eg:
    #   <deployment-id>:<revision>
    # the login.py module parses this and creates the ident record. If <revision> is given in CLI
    # the ident record passed in from click contains _revision value
    # The user_session._ident_record() returns the same dict
    if revision:
        mock_ident["_revision"] = revision
    request_params: dict = {"ident": mock_ident, "format": "json"}
    mock_resp: dict = {
        "status_text": "Started",
        "resource_profile": kwargs_attr.get("resource_profile", mock_ident["resource_profile"]),
    }
    user_session._ident_record = MagicMock(return_value=mock_ident)
    # user_session._ident_record = MagicMock(_ident=mock_ident)
    user_session._patch = MagicMock(
        return_value=mock_resp,
    )
    if rev := mock_ident.get("_revision", None):
        if rev == "does-not-exist":
            mock_get_records = MagicMock(side_effect=[Exception("Error: No revisions found matching name=".format(mock_ident["_revision"]))])
        else:
            proj_ident = mock_ident.get("project", mock_ident)
            # latest gets written to the actual latest tag
            rev = latest if rev == "latest" else rev
            mock_get_records = MagicMock(
                return_value={
                    "name": rev,
                    "id": rev,
                    "project_id": proj_ident["id"],
                }
            )
        user_session._get_records = mock_get_records

    user_session.deployment_patch(**request_params, **kwargs_attr)

    # deployment_patch invokes -> _patch API call only if there are changes
    _patch_called = False
    for k, v in kwargs_attr.items():
        if mock_ident[k] != v:
            _patch_called = True
            break
    # if revision = 'latest'; then revision is same as mock_ident; so _patch should not be called
    if revision != "latest" and revision != mock_ident["revision"]:
        _patch_called = True

    if _patch_called:
        user_session._patch.assert_called_once()
    else:
        user_session._patch.assert_not_called()
