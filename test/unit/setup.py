import shlex
import subprocess

from dotenv import load_dotenv

# load locally defined environmental variables
load_dotenv(dotenv_path="env/env.local")  # take environment variables from .env.


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
    cmd: str = "coverage run --append --rcfile=.coveragerc -m unittest discover test/unit/anaconda/sdk"
    stdout, stderr, returncode = shell_out(shell_out_cmd=cmd)
    print(stdout)
    print(stderr)
    if returncode != 0:
        raise Exception("Unit Test Execute Failed")
