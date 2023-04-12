import click

from ..login import cluster_call
from ..utils import global_options, ident_filter


@click.group(
    short_help="info, list", epilog='Type "ae5 resource-profile <command> --help" for help on a specific command.'
)
@global_options
def resource_profile():
    """Commands related to resource profiles."""
    pass


@resource_profile.command()
@ident_filter("resource_profile")
@global_options
def list(**kwargs):
    """List all availables resource profiles."""
    cluster_call("resource_profile_list", **kwargs)


@resource_profile.command()
@ident_filter("resource_profile", required=True)
@global_options
def info(**kwargs):
    """Retrieve the record of a single resource profile.

    The NAME identifier must match exactly one name of a resource profile.
    Wildcards may be included.
    """
    cluster_call("resource_profile_info", **kwargs)
