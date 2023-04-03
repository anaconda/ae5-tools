from fastapi import status
from starlette.exceptions import HTTPException

from ...common.config.environment import demand_env_var
from ...common.config.environment_variable_not_found_error import EnvironmentVariableNotFoundError
from ...common.secrets import load_ae5_user_secrets


class SecretsCommand:
    @staticmethod
    def get(name: str) -> str:
        load_ae5_user_secrets(silent=False)
        try:
            return demand_env_var(name=name)
        except EnvironmentVariableNotFoundError as error:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error
