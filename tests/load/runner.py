""" """

from __future__ import annotations

from ae5_tools import demand_env_var_as_bool, get_env_var
from tests.adsp.common.utils import _process_launch_wait


def run() -> None:
    shell_out_cmd: str = "python -m pytest --cov=ae5_tools --show-capture=all -rP tests/load/ae5_tools --cov-append --cov-report=xml -vv"

    if get_env_var(name="CI") and demand_env_var_as_bool(name="CI"):
        shell_out_cmd += " --ci-skip"

    _process_launch_wait(shell_out_cmd=shell_out_cmd)


if __name__ == "__main__":
    run()
