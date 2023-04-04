from typing import Dict, List

from ae5_tools.api import AEUserSession
from tests.utils import _cmd, _get_vars, CMDException
import pytest
import uuid


@pytest.fixture(scope='session')
def user_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_USERNAME', 'AE5_PASSWORD')
    s = AEUserSession(hostname, username, password)
    yield s
    s.disconnect()


@pytest.fixture(scope="module")
def secret_name():
    return str(uuid.uuid4()).replace("-", "_")

#####################################################
# Test Cases For secret_add, secret_list, and secret_delete
#####################################################


def test_secret_create_and_list_and_delete(user_session, secret_name):
    secret_value: str = str(uuid.uuid4())
    secret_add_result: str = _cmd("secret", "add", secret_name, secret_value)
    assert secret_add_result == ''

    user_secrets: List[str] = user_session.secret_list()
    assert secret_name in [secret["secret_name"] for secret in user_secrets]

    secret_delete_result: str = _cmd("secret", "delete", secret_name)
    assert secret_delete_result == ''


def test_secret_delete_gracefully_fails(user_session):
    secret_key: str = str(uuid.uuid4()).replace("-", "_")
    with pytest.raises(CMDException) as context:
         _cmd("secret", "delete", secret_key)
    assert f"User secret {secret_key} was not found and cannot be deleted." in str(context.value)
