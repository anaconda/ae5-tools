"""Docker utilities"""

import os
import shutil
import subprocess
from os import path

from src.anaconda.enterprise.sdk.common.config.environment import get_env_var


def get_condarc(custom_path):
    """return contents of condarc

    The following locations are sanned in order
    for the condarc file

    1. provided path
    2. ~/.ae5/condarc
    3. <site-packages>/ae5_tools/condarc.dist (copied to #2)
    """

    _path = path.expanduser(get_env_var("AE5_TOOLS_CONFIG_DIR") or "~/.ae5")
    user_file = path.join(_path, "condarc")
    dist_file = path.join(path.dirname(__file__), "condarc.dist")

    if custom_path:
        if path.exists(custom_path):
            condarc = custom_path
        else:
            msg = f"Custom condarc file {custom_path} was not found."
            raise FileNotFoundError(msg)

    elif path.exists(user_file):
        condarc = user_file
    elif path.exists(dist_file):
        shutil.copy(dist_file, user_file)
        condarc = user_file
    else:
        msg = f"""condarc was not found in any of the following locations
{user_file}
{dist_file}
Please check that the file exists or that ae5-tools was installed properly.
Otherwise, please file a bug report."""
        raise FileNotFoundError(msg)

    with open(condarc) as f:
        contents = f.read()
    return contents


def get_dockerfile(custom_path=None):
    """return contents of dockerfile

    The following locations are scanned in order
    for the Dockerfile
    1. provided path
    2. ~/.ae5/Dockerfile
    3. <site-packages>/ae5_tools/Dockerfile.dist (copied to #2)
    """

    _path = path.expanduser(get_env_var("AE5_TOOLS_CONFIG_DIR") or "~/.ae5")
    user_file = path.join(_path, "Dockerfile")
    dist_file = path.join(path.dirname(__file__), "Dockerfile.dist")

    if custom_path:
        if path.exists(custom_path):
            dockerfile = custom_path
        else:
            msg = f"Custom Dockerfile {custom_path} was not found."
            raise FileNotFoundError(msg)

    elif path.exists(user_file):
        dockerfile = user_file
    elif path.exists(dist_file):
        shutil.copy(dist_file, user_file)
        dockerfile = user_file
    else:
        msg = f"""Dockerfile was not found in any of the following locations
{user_file}
{dist_file}
Please check that the file exists or that ae5-tools was installed properly.
Otherwise, please file a bug report."""
        raise FileNotFoundError(msg)

    with open(dockerfile) as f:
        contents = f.read()
    return contents


def build_image(path, tag, **build_args):
    print(f"*** {tag} image build starting")

    cmd = ["docker", "build", path, "-t", tag]
    for arg, value in build_args.items():
        cmd.append("--" + arg)
        cmd.append(value)

    subprocess.check_call(cmd)
