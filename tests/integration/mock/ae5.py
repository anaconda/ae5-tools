import uuid
from typing import Any, Dict, Optional
import logging
from fastapi import FastAPI, status
from pydantic.main import BaseModel
from starlette.exceptions import HTTPException
from starlette.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.DEBUG)


class MockState:
    get_token: Optional[Dict]
    get_users: Optional[Dict]
    get_events: Optional[Dict]
    get_realm_roles: Optional[Dict]

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.reset()

    def reset(self):
        self.get_token = {
            "calls": [],
            "responses": []
        }
        self.get_users = {
            "calls": [],
            "responses": []
        }
        self.get_events = {
            "calls": [],
            "responses": []
        }
        self.get_realm_roles = {
            "calls": [],
            "responses": []
        }

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


# Define our handlers


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


@app.post(path="/auth/realms/master/protocol/openid-connect/token")
def get_token():
    mock_state.get_token["calls"].append({})
    return mock_state.get_token["responses"].pop(0)


@app.get(path="/auth/admin/realms/AnacondaPlatform/users")
def get_users():
    mock_state.get_users["calls"].append({})
    return mock_state.get_users["responses"].pop(0)


@app.get(path="/auth/admin/realms/AnacondaPlatform/events")
def get_events():
    mock_state.get_events["calls"].append({})
    return mock_state.get_events["responses"].pop(0)


@app.get(path="/auth/admin/realms/AnacondaPlatform/users/{user_uuid}/role-mappings/realm")
def get_realm_roles(user_uuid: str):
    mock_state.get_realm_roles["calls"].append({"user_uuid": user_uuid})
    return mock_state.get_realm_roles["responses"].pop(0)


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
        "get_realm_roles": mock_state.get_realm_roles
    }


@app.patch(path="/mock/state")
def patch_state(new_partial_state: Dict) -> Dict:
    for key, value in new_partial_state.items():
        if key == "get_token":
            mock_state.get_token = {
                **mock_state.get_token,
                **value
            }
        elif key == "get_users":
            mock_state.get_users = {
                **mock_state.get_users,
                **value
            }
        elif key == "get_events":
            mock_state.get_events = {
                **mock_state.get_events,
                **value
            }
        elif key == "get_realm_roles":
            mock_state.get_realm_roles = {
                **mock_state.get_realm_roles,
                **value
            }
        else:
            raise NotImplementedError()
    return get_state()