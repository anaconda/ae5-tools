import shlex
import subprocess

from ..contracts.dto.launch_parameters import LaunchParameters


class AESecretController:
    @staticmethod
    def _process_launch_wait(shell_out_cmd: str) -> None:
        args = shlex.split(shell_out_cmd)

        with subprocess.Popen(args, stdout=subprocess.PIPE) as process:
            for line in iter(process.stdout.readline, b""):
                print(line)

    def execute(self, params: LaunchParameters) -> None:
        AESecretController.launch_server(params=params)

    @staticmethod
    def launch_server(params: LaunchParameters) -> None:
        cmd: str = "uvicorn src.api.main:app " f"--host {params.address} " f"--port {params.port}"
        print(cmd)
        AESecretController._process_launch_wait(shell_out_cmd=cmd)
