from typing import Dict, List
import uuid

import pytest

from ae5_tools.api import AEUserSession, AEException
from tests.utils import _get_vars


@pytest.fixture(scope='session')
def user_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_USERNAME', 'AE5_PASSWORD')
    s = AEUserSession(hostname, username, password)
    yield s
    s.disconnect()


@pytest.fixture(scope="module")
def secret_name():
    return "MOCK_SECRET"


#####################################################
# Test Cases For secret_add, secret_list, and secret_delete
#####################################################

def test_secret_add_and_list(user_session, secret_name):
    user_session.secret_add(key=secret_name, value=str(uuid.uuid4()))
    results: List[str] = user_session.secret_list()
    secrets: List[str] = results[0]["secrets"]
    assert secret_name in secrets


def test_secret_delete(user_session, secret_name):
    user_session.secret_delete(key=secret_name)


def test_secret_delete_should_gracefully_fail(user_session, secret_name):
    new_key: str = str(uuid.uuid4())
    with pytest.raises(AEException) as context:
        user_session.secret_delete(key=new_key)
    assert str(context.value) == f"User secret {new_key!r} was not found and cannot be deleted."
