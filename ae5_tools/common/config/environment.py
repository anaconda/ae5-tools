""" Helper functions for environment variables. """
from __future__ import annotations

import os

from ..contracts.errors.environment_variable_not_found_error import EnvironmentVariableNotFoundError


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
        raise EnvironmentVariableNotFoundError("Environment variable (%s) not found", name)
    return os.environ[name]


def get_env_var(name: str) -> str | None:
    """
    Returns an environment variable as a string, otherwise `None` if it does not exist.

    Parameters
    ----------
    name: str
        The name of the environment variable.

    Returns
    -------
        value: str | None
            The environment variables value as a string, or `None` if it does not exist.
    """

    if name not in os.environ:
        return None
    return os.environ[name]


def demand_env_var_as_bool(name: str) -> bool:
    """
    Returns an environment variable as a bool, or throws an exception.

    Parameters
    ----------
    name: str
        The name of the environment variable.

    Returns
    -------
        The environment variables value as a bool.
    """

    value_str: str = demand_env_var(name=name).lower()
    if value_str in ("true", "1"):
        return True
    if value_str in ("false", "0"):
        return False
    raise EnvironmentVariableNotFoundError("Environment variable (%s) not boolean and can not be loaded", name)
