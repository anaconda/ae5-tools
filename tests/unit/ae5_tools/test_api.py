import json
import os
from unittest.mock import MagicMock

import requests

from ae5_tools.api import AEAdminSession, AESessionBase, AEUserSession

base_params: dict = {"hostname": "mock-hostname", "username": "mock-username", "password": "<PASSWORD>", "persist": False}


def unset_cf_env_vars():
    if "CF_ACCESS_CLIENT_ID" in os.environ:
        del os.environ["CF_ACCESS_CLIENT_ID"]
    if "CF_ACCESS_CLIENT_SECRET" in os.environ:
        del os.environ["CF_ACCESS_CLIENT_SECRET"]


def set_cf_env_vars():
    os.environ["CF_ACCESS_CLIENT_ID"] = "MOCK-CLIENT-ID"
    os.environ["CF_ACCESS_CLIENT_SECRET"] = "MOCK-CLIENT-SECRET"


class AESessionBaseTester(AESessionBase):
    _connected = MagicMock(return_value=True)
    _connect = MagicMock(return_value=True)
    _set_header = MagicMock(return_value=True)

    def __init__(self, hostname, username, password, prefix, persist, **kwargs):
        super().__init__(hostname, username, password, prefix, persist)


def test_cloudformation_headers_are_added_if_present():
    # Ensure CloudFormation CF Header code path is executed
    set_cf_env_vars()

    tester: AESessionBaseTester = AESessionBaseTester(**base_params, prefix="mock-prefix")

    assert tester.session.headers["CF-Access-Client-Id"] == "MOCK-CLIENT-ID"
    assert tester.session.headers["CF-Access-Client-Secret"] == "MOCK-CLIENT-SECRET"

    del tester
    unset_cf_env_vars()

    tester: AESessionBaseTester = AESessionBaseTester(**base_params, prefix="mock-prefix")
    assert "CF-Access-Client-Id" not in tester.session.headers
    assert "CF-Access-Client-Secret" not in tester.session.headers


def test_cloudformation_headers_are_included_on_authorize():
    unset_cf_env_vars()

    tester: AESessionBaseTester = AESessionBaseTester(**base_params, prefix="mock-prefix")
    assert "CF-Access-Client-Id" not in tester.session.headers
    assert "CF-Access-Client-Secret" not in tester.session.headers

    set_cf_env_vars()
    tester.authorize()
    assert tester.session.headers["CF-Access-Client-Id"] == "MOCK-CLIENT-ID"
    assert tester.session.headers["CF-Access-Client-Secret"] == "MOCK-CLIENT-SECRET"

    unset_cf_env_vars()


def test_user_session_added_cloudflare_headers_if_present():
    unset_cf_env_vars()

    user_session: AEUserSession = AEUserSession(**base_params)
    user_session._set_header()
    assert "CF-Access-Client-Id" not in user_session.session.headers
    assert "CF-Access-Client-Secret" not in user_session.session.headers
    del user_session

    set_cf_env_vars()
    user_session: AEUserSession = AEUserSession(**base_params)
    user_session._set_header()
    assert user_session.session.headers["CF-Access-Client-Id"] == "MOCK-CLIENT-ID"
    assert user_session.session.headers["CF-Access-Client-Secret"] == "MOCK-CLIENT-SECRET"

    unset_cf_env_vars()


def test_admin_session_connect_successes_with_cloudflare():
    admin_session: AEAdminSession = AEAdminSession(**base_params)
    admin_session.session = MagicMock(return_value=True)

    mock_response: MagicMock = MagicMock()
    mock_response.status_code = 200
    mock_response.json = MagicMock(return_value={"key": "value"})
    admin_session.session.post = MagicMock(return_value=mock_response)

    admin_session._connect(password=base_params["password"])
    assert admin_session._sdata == {"key": "value"}


def test_admin_session_gracefully_failures_deserializing_cloudflare_error():
    admin_session: AEAdminSession = AEAdminSession(**base_params)
    resp: requests.Response = requests.Response()
    resp.status_code = 200
    resp.json = MagicMock(side_effect=[json.decoder.JSONDecodeError("Boom!", "", 0)])
    admin_session.session.post = MagicMock(return_value=resp)
    admin_session._connect(password=base_params["password"])
    assert admin_session._sdata == {}


def test_admin_session_gracefully_failures_on_cloudflare_error():
    admin_session: AEAdminSession = AEAdminSession(**base_params)
    resp: requests.Response = requests.Response()
    resp.status_code = 200
    resp.json = MagicMock(side_effect=[Exception("Boom!")])
    admin_session.session.post = MagicMock(return_value=resp)
    admin_session._connect(password=base_params["password"])
    assert admin_session._sdata == {}


def test_admin_session_gracefully_failures_with_exceeded_retry_count():
    admin_session: AEAdminSession = AEAdminSession(**base_params)
    admin_session.session.post = MagicMock(side_effect=[requests.exceptions.RetryError("Boom!")])
    admin_session._connect(password=base_params["password"])
    assert admin_session._sdata == {}
