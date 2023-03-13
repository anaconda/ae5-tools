import uuid
from unittest.mock import MagicMock

import pytest
from ae5_tools.api import AEUserSession


@pytest.fixture(scope="function")
def get_token_fixture():
    return {
        "access_token": str(uuid.uuid4()),
        "refresh_token": str(uuid.uuid4()),
    }


@pytest.fixture(scope="function")
def user_session(get_token_fixture):
    user_session = AEUserSession(
        hostname="MOCK-HOSTNAME", username="MOCK-AE-USERNAME", password="MOCK-AE-USER-PASSWORD"
    )
    return user_session


#####################################################
# Test Cases For job_run
#####################################################


def test_job_run(user_session):
    job_id: str = str(uuid.uuid4())
    user_session.connected = True
    user_session._ident_record = MagicMock(return_value={"id": job_id})
    user_session._post = MagicMock(return_value={"id": job_id })
    response = user_session.job_run(ident="mock-id")
    assert response["id"] == job_id
