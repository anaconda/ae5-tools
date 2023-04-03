from fastapi import FastAPI, status
from starlette.middleware.cors import CORSMiddleware

from .commands.secrets_command import SecretsCommand

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


@app.get("/secrets", status_code=status.HTTP_200_OK)
def get_secrets(name: str) -> str:
    return SecretsCommand.get(name=name)
