""" Helpers for working with Anaconda Enterprise Secrets """

import os
from pathlib import Path


def load_ae5_user_secrets(secrets_path: str = "/var/run/secrets/user_credentials/", silent: bool = True) -> None:
    """
    Load Anaconda Enterprise user secrets from the file system into environment values for the current session.

    Parameters
    ----------
    secrets_path: str
        The file system location to look for Anaconda Enterprise user level secrets.  The default value should
        be used unless you have a specific reason to change it.
    silent: bool
        Flag for controlling logging.
    """

    base_path: Path = Path(secrets_path)
    if base_path.exists():
        if not silent:
            print(f"Loading environment variables from {base_path}:")
        for secret in base_path.glob("*"):
            if secret.is_file():
                if not silent:
                    print(secret.name)
                with open(file=secret, mode="r", encoding="utf-8") as file:
                    os.environ[secret.name] = file.read()
    else:
        if not silent:
            print(f"Skipping loading AE5 user secrets, specified secrets path not found: {secrets_path}")
