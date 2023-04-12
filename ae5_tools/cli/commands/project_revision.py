import click

from ...identifier import Identifier
from ..login import cluster_call
from ..utils import global_options, ident_filter


@click.group(
    short_help="Subcommands: commands, download, image, info, list",
    epilog='Type "ae5 project revision <command> --help" for help on a specific command.',
)
@global_options
def revision():
    """Commands related to the revisions of a project."""
    pass


@revision.command()
@ident_filter("project", required=True, handle_revision=True)
@global_options
def list(**kwargs):
    """List available revisions for a given project.

    The PROJECT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.
    """
    cluster_call("revision_list", **kwargs)


@revision.command()
@ident_filter("project", required=True, handle_revision=True)
@global_options
def info(**kwargs):
    """Retrieve information about a single project revision.

    The REVISION identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project. If the revision is not
    specified, then the latest will be assumed.
    """
    cluster_call("revision_info", **kwargs)


@revision.command()
@ident_filter("project", required=True, handle_revision=True)
@global_options
def commands(**kwargs):
    """List the commands for a given project revision.

    The REVISION identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project. If the revision is not
    specified, then the latest will be assumed.
    """
    cluster_call("revision_commands", **kwargs)


def _download(**kwargs):
    file_s = f' to {kwargs["filename"]}' if kwargs.get("filename") else ""
    cluster_call(
        "project_download", **kwargs, prefix=f"Downloading project {{ident}}{file_s}...", postfix="downloaded."
    )


@revision.command()
@ident_filter("project", required=True, handle_revision=True)
@click.option("--filename", default="", help="Filename")
@global_options
def download(**kwargs):
    """Download a project revision.

    The REVISION identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project. If the revision is not
    specified, the latest will be assumed.
    """
    _download(**kwargs)


def _image(**kwargs):
    cluster_call("project_image", **kwargs)


@revision.command()
@ident_filter("project", required=True, handle_revision=True)
@click.option("--command", default="", help="Command name to execute.")
@click.option("--condarc", default="", help="Path to custom condarc file.")
@click.option("--dockerfile", default="", help="Path to custom Dockerfile.")
@click.option("--debug", is_flag=True, help="debug logs")
@global_options
def image(**kwargs):
    _image(**kwargs)
