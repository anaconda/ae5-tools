import shlex
import subprocess
from pathlib import Path
from typing import Optional

import requests
import os
import time

from dotenv import load_dotenv


def start_ae5_mock() -> subprocess.Popen:
    cmd: str = f"uvicorn tests.integration.mock.ae5:app --host {os.environ['AE5_HOSTNAME']} --port 443 --ssl-keyfile tests/integration/mock/certs/nginx.key --ssl-certfile tests/integration/mock/certs/nginx.crt"
    args = shlex.split(cmd)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def wait_for_ae5_mock() -> None:
    time_out: int = 1
    max_retry_count: int = 100
    count: int = 0

    time.sleep(time_out)
    while count < max_retry_count:
        count += 1

        try:
            response = requests.get(url=f"https://{os.environ['AE5_HOSTNAME']}:443/health/plain", verify=False)

            if response.status_code != 200:
                print(f"Received status code: {response.status_code}, sleeping ..")
                time.sleep(time_out)
            else:
                print("Saw 200 status code, moving on..")
                break

        except ConnectionRefusedError as error:
            print(f"Received exception ConnectionRefusedError, sleeping ...")
            time.sleep(time_out)

    if count >= max_retry_count:
        raise Exception("Mock did not come online")


def shell_out(shell_out_cmd: str) -> tuple[str, str, int]:
    args = shlex.split(shell_out_cmd)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        outs, errs = proc.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
    return outs.decode(encoding="utf-8"), errs.decode(encoding="utf-8"), proc.returncode


if __name__ == "__main__":
    # load locally defined environmental variables
    local_env_config: str = (Path(os.path.dirname(os.path.realpath(__file__))) / "env.offline").as_posix()
    load_dotenv(dotenv_path=local_env_config, override=True)  # take environment variables from .env.

    ae5_mock: Optional[subprocess.Popen] = None

    try:
        # Start Mocks
        ae5_mock = start_ae5_mock()
        wait_for_ae5_mock()

        # Start Tests
        cmd: str = "py.test --cov=ae5_tools -v tests/integration --cov-append --cov-report=xml --show-capture=all -rP"
        stdout, stderr, returncode = shell_out(shell_out_cmd=cmd)
    except Exception as error:
        # Tear Down Mocks
        if ae5_mock is not None:
            ae5_mock.terminate()
            ae5_mock = None
        raise error from error
    finally:
        # Tear Down Mocks
        if ae5_mock is not None:
            ae5_mock.terminate()
            ae5_mock = None

    # Report Test Results
    print(stdout)
    print(stderr)
    if returncode != 0:
        raise Exception("Integration Test Execute Failed")
