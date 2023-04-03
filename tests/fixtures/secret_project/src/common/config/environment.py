""" Helper functions for environment variables. """

import os

from .environment_variable_not_found_error import EnvironmentVariableNotFoundError


def demand_env_var(name: str) -> str:
    """
    Returns an environment variable as a string, or throws an exception.

    Parameters
    ----------
    name: str
        The name of the environment variable.

    Returns
    -------
        The environment variables value as a string.
    """

    if name not in os.environ:
        raise EnvironmentVariableNotFoundError(f"Environment variable ({name}) not found")
    return os.environ[name]
