import re
import webbrowser

import click

from ..login import cluster_call
from ..utils import global_options, ident_filter, yes_option
from .deployment_collaborator import collaborator


@click.group(
    short_help="collaborator, info, list, open, patch, restart, start, stop",
    epilog='Type "ae5 deployment <command> --help" for help on a specific command.',
)
@global_options
def deployment():
    """Commands related to project deployments."""
    pass


deployment.add_command(collaborator)


@deployment.command()
@ident_filter("deployment")
@click.option(
    "--collaborators",
    is_flag=True,
    help="Include collaborators. Since this requires an API call for each project, it can be slow if there are large numbers of projects.",
)
@click.option("--k8s", is_flag=True, help="Include Kubernetes-derived columns (requires additional API calls).")
@global_options
def list(**kwargs):
    """List available deployments.

    By default, lists all deployments visible to the authenticated user.
    Simple filters on owner, project name, or id can be performed by
    supplying an optional DEPLOYMENT argument. Filters on other fields may
    be applied using the --filter option.
    """
    cluster_call("deployment_list", **kwargs)


@deployment.command()
@ident_filter("deployment", required=True)
@click.option("--k8s", is_flag=True, help="Include Kubernetes-derived columns (requires additional API calls).")
@global_options
def info(**kwargs):
    """Retrieve information about a single deployment.

    The DEPLOYMENT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.
    """
    cluster_call("deployment_info", **kwargs)


@deployment.command()
@ident_filter("deployment", required=True)
@click.option("--app", "which", flag_value="app", default=True, help="Return the app log (default).")
@click.option("--proxy", "which", flag_value="proxy", help="Return the proxy log.")
@click.option("--events", "which", flag_value="events", help="Return the event log.")
@global_options
def logs(**kwargs):
    """Retrieve the logs for a deployment.

    The DEPLOYMENT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.
    """
    cluster_call("deployment_logs", **kwargs)


@deployment.command()
@ident_filter("deployment", required=True)
@global_options
def token(**kwargs):
    """Retrieve a bearer token to access a private deployment.

    The DEPLOYMENT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.
    """
    cluster_call("deployment_token", **kwargs)


@deployment.command()
@ident_filter("deployment", required=True)
@click.option("--public/--private", is_flag=True, default=None, help="Make the deployment public/private (default).")
@global_options
def patch(**kwargs):
    """Change a deployment's public/private status.

    The DEPLOYMENT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one deployment.
    """
    cluster_call("deployment_patch", **kwargs)


def _start(**kwargs):
    name_s = f' {kwargs["name"]}' if kwargs.get("name") else ""
    endpoint_s = f' at endpoint {kwargs["endpoint"]}' if kwargs.get("endpoint") else ""
    public_s = "public" if kwargs.get("public") else "private"
    response = cluster_call(
        "deployment_start",
        **kwargs,
        prefix=f"Starting {public_s} deployment{name_s}{endpoint_s} for {{ident}}...",
        postfix="started.",
    )


@deployment.command()
@ident_filter(name="project", handle_revision=True, required=True)
@click.option(
    "--name",
    type=str,
    default=None,
    required=False,
    help="Deployment name. If not supplied, it is autogenerated from the project name.",
)
@click.option("--endpoint", type=str, required=False, help="Endpoint name. If not supplied, a generated subdomain will be used.")
@click.option("--command", help="The command to use for this deployment.")
@click.option("--resource-profile", help="The resource profile to use for this deployment.")
@click.option("--public/--private", is_flag=True, help="Make the deployment public/private (default).")
@click.option("--wait", is_flag=True, help="Wait for the deployment to complete initialization before exiting.")
@click.option("--stop-on-error", is_flag=True, help="Stop the deployment if it fails on the first attempt. Implies --wait.")
@click.option("--open", is_flag=True, help="Open a browser upon initialization. Implies --wait.")
@click.option("--frame", is_flag=True, help="Include the AE banner when opening.")
@global_options
def start(**kwargs):
    """Start a deployment for a project.

    The PROJECT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.

    If the static endpoint is supplied, it must be of the form r'[A-Za-z0-9-]+',
    and it will be converted to lowercase. It must not match any endpoint with
    an active deployment, nor can it match any endpoint claimed by another project,
    even if that project has no active deployments. If the endpoint is not supplied,
    it will be autogenerated from the project name.

    By default, this command will wait for the completion of the deployment
    creation before returning. To return more quickly, use the --no-wait option.
    """
    response = _start(**kwargs)


@deployment.command()
@ident_filter("deployment", required=True)
@click.option("--wait", is_flag=True, help="Wait for the deployment to complete initialization before exiting.")
@click.option("--stop-on-error", is_flag=True, help="Stop the deployment if it fails on the first attempt. Implies --wait.")
@click.option("--open", is_flag=True, help="Open a browser upon initialization. Implies --wait.")
@click.option("--frame", is_flag=True, help="Include the AE banner when opening.")
@global_options
def restart(**kwargs):
    """Restart a deployment for a project.

    The DEPLOYMENT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.
    """
    result = cluster_call("deployment_restart", **kwargs, prefix="Restarting deployment {ident}...", postfix="restarted.")


@deployment.command(short_help="Stop a deployment.")
@ident_filter("deployment", required=True)
@yes_option
@global_options
def stop():
    """Stop a deployment.

    The DEPLOYMENT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.
    """
    cluster_call("deployment_stop", confirm="Stop deployment {ident}", prefix="Stopping {ident}...", postfix="stopped.")


@deployment.command(short_help="Open a deployment in a browser.")
@ident_filter("deployment", required=True)
@click.option("--frame", is_flag=True, help="Include the AE banner when opening.")
@global_options
def open(**kwargs):
    """Opens a deployment in the default browser.

    The DEPLOYMENT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one session.

    For deployments, the frameless version of the deployment will be opened by
    default. If you wish to the Anaconda Enterprise banner at the top
    of the window, use the --frame option.
    """
    cluster_call("deployment_open", **kwargs)
