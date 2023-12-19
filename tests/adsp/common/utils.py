from __future__ import annotations

import logging
import shlex
import subprocess

logger = logging.getLogger(__name__)


def _process_launch_wait(shell_out_cmd: str, cwd: str = ".") -> None:
    args = shlex.split(shell_out_cmd)

    try:
        with subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE) as process:
            for line in iter(process.stdout.readline, b""):
                logger.info(line)

        if process.returncode != 0:
            raise Exception("subprocess failed")
    except Exception as error:
        # Catch and handle ALL errors
        logger.error("Exception was caught while executing task.")
        logger.error(str(error))
        raise error
