from __future__ import annotations

import logging

from dotenv import load_dotenv

from ae5_tools import demand_env_var_as_bool, get_env_var
from tests.adsp.common.utils import _process_launch_wait

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run() -> None:
    shell_out_cmd: str = "python -m pytest --cov=ae5_tools --show-capture=all -rP tests/load/ae5_tools --cov-append --cov-report=xml -vv"

    if get_env_var(name="CI") and demand_env_var_as_bool(name="CI"):
        shell_out_cmd += " --ci-skip"

    logger.info("Test Runner Configuration Complete")
    logger.info(f"Executing: {shell_out_cmd}")
    _process_launch_wait(shell_out_cmd=shell_out_cmd)


if __name__ == "__main__":
    # Load env vars, - do NOT override previously defined ones
    load_dotenv(override=False)

    run()
