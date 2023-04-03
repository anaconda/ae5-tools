""" Anaconda Enterprise Service Wrapper Definition """

import sys
from argparse import ArgumentParser, Namespace

from ..common.secrets import load_ae5_user_secrets
from ..contracts.dto.launch_parameters import LaunchParameters
from .controller import AESecretController

if __name__ == "__main__":
    # This function is meant to provide a handler mechanism between the AE5 deployment arguments
    # and those required by the called process (or service).

    # arg parser for the standard anaconda-project options
    parser = ArgumentParser(
        prog="anaconda-enterprise-secret-tester",
        description="anaconda enterprise secret tester",
    )
    parser.add_argument("--anaconda-project-host", action="append", default=[], help="Hostname to allow in requests")
    parser.add_argument("--anaconda-project-port", action="store", default=8086, type=int, help="Port to listen on")
    parser.add_argument(
        "--anaconda-project-iframe-hosts",
        action="append",
        help="Space-separated hosts which can embed us in an iframe per our Content-Security-Policy",
    )
    parser.add_argument(
        "--anaconda-project-no-browser", action="store_true", default=False, help="Disable opening in a browser"
    )
    parser.add_argument(
        "--anaconda-project-use-xheaders", action="store_true", default=False, help="Trust X-headers from reverse proxy"
    )
    parser.add_argument("--anaconda-project-url-prefix", action="store", default="", help="Prefix in front of urls")
    parser.add_argument(
        "--anaconda-project-address",
        action="store",
        default="0.0.0.0",
        help="IP address the application should listen on",
    )

    # Load command line arguments
    args: Namespace = parser.parse_args(sys.argv[1:])
    print(args)

    # load defined environmental variables
    load_ae5_user_secrets(silent=False)

    # Create our controller
    controller: AESecretController = AESecretController()

    # Build launch parameters
    params: LaunchParameters = LaunchParameters(port=args.anaconda_project_port, address=args.anaconda_project_address)

    # Execute the request
    controller.execute(params=params)
