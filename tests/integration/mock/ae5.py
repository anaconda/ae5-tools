"""
Dynamic AE5 Integration Test Mock

A very basic dynamic network mock for AE5.
"""

import logging
import os
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, status
from starlette.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


class MockState:
    """Holds dynamic mock state in memory"""

    get_token: Optional[Dict]
    get_users: Optional[Dict]
    get_events: Optional[Dict]
    get_realm_roles: Optional[Dict]

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.reset()

    def reset(self):
        self.get_token = {"calls": [], "responses": []}
        self.get_users = {"calls": [], "responses": []}
        self.get_events = {"calls": [], "responses": []}
        self.get_realm_roles = {"calls": [], "responses": []}


class AE5MockClient:
    """
    Provides a client for the ae5 mock.
    This is used by integration tests for setup and tear down.
    """

    @staticmethod
    def reset_mock_state() -> bool:
        response = requests.delete(url=f"https://{os.environ['AE5_HOSTNAME']}/mock/state", verify=False)
        if response.status_code != 200:
            message: str = f"reset_mock_state saw: {response.status_code}, text: {response.text}"
            raise Exception(message)
        return True

    @staticmethod
    def set_mock_state(mock_state: Dict) -> Dict:
        response = requests.patch(url=f"https://{os.environ['AE5_HOSTNAME']}/mock/state", json=mock_state, verify=False)
        if response.status_code != 200:
            message: str = f"set_mock_state saw: {response.status_code}, text: {response.text}"
            raise Exception(message)
        return response.json()

    @staticmethod
    def get_mock_state() -> Dict:
        response = requests.get(url=f"https://{os.environ['AE5_HOSTNAME']}/mock/state", verify=False)
        if response.status_code != 200:
            message: str = f"get_mock_state saw: {response.status_code}, text: {response.text}"
            raise Exception(message)
        return response.json()


###############################################################################
#  Set up application server
###############################################################################

mock_state: MockState = MockState()

# Create the FastAPI application
app = FastAPI()

#  Apply COR Configuration | https://fastapi.tiangolo.com/tutorial/cors/
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


###############################################################################
# Define our handlers
###############################################################################

@app.get("/health/plain", status_code=status.HTTP_200_OK)
def health_plain() -> bool:
    """
    Get Application Health
    [GET] /health/plain
    Returns
    -------
    [STATUS CODE] 200: OK
        health: bool
            A true/false response of server health.
    """

    return True


@app.delete(path="/mock/state", status_code=status.HTTP_200_OK)
def reset():
    mock_state.reset()
    return get_state()


@app.get(path="/mock/state")
def get_state():
    return {
        "get_token": mock_state.get_token,
        "get_users": mock_state.get_users,
        "get_events": mock_state.get_events,
        "get_realm_roles": mock_state.get_realm_roles,
    }


@app.patch(path="/mock/state")
def patch_state(new_partial_state: Dict) -> Dict:
    for key, value in new_partial_state.items():
        if key == "get_token":
            mock_state.get_token = {**mock_state.get_token, **value}
        elif key == "get_users":
            mock_state.get_users = {**mock_state.get_users, **value}
        elif key == "get_events":
            mock_state.get_events = {**mock_state.get_events, **value}
        elif key == "get_realm_roles":
            mock_state.get_realm_roles = {**mock_state.get_realm_roles, **value}
        else:
            raise NotImplementedError()
    return get_state()



###############################################################################
# Define our mocked endpoints
###############################################################################


@app.post(path="/auth/realms/master/protocol/openid-connect/token")
def get_token():
    mock_state.get_token["calls"].append({})
    try:
        return mock_state.get_token["responses"].pop(0)
    except Exception as error:
        raise HTTPException(status_code=500, detail={"message": "Missing mock call definition"}) from error


@app.get(path="/auth/admin/realms/AnacondaPlatform/users")
def get_users():
    mock_state.get_users["calls"].append({})
    try:
        return mock_state.get_users["responses"].pop(0)
    except Exception as error:
        raise HTTPException(status_code=500, detail={"message": "Missing mock call definition"}) from error

@app.get(path="/auth/admin/realms/AnacondaPlatform/events")
def get_events():
    mock_state.get_events["calls"].append({})
    try:
        return mock_state.get_events["responses"].pop(0)
    except Exception as error:
        raise HTTPException(status_code=500, detail={"message": "Missing mock call definition"}) from error

@app.get(path="/auth/admin/realms/AnacondaPlatform/users/{user_uuid}/role-mappings/realm")
def get_realm_roles(user_uuid: str):
    mock_state.get_realm_roles["calls"].append({"user_uuid": user_uuid})
    try:
        return mock_state.get_realm_roles["responses"].pop(0)
    except Exception as error:
        raise HTTPException(status_code=500, detail={"message": "Missing mock call definition"}) from error

