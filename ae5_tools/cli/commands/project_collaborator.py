import click

from ..login import cluster_call
from ..utils import global_options, ident_filter


@click.group(
    short_help="Subcommands: add, info, list, remove",
    epilog='Type "ae5 project collaborator <command> --help" for help on a specific command.',
)
@global_options
def collaborator():
    """Commands related to the collaborators on a project."""
    pass


@collaborator.command()
@ident_filter("project", required=True)
@global_options
def list(**kwargs):
    """List the collaborators on a project.

    The PROJECT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.
    """
    cluster_call("project_collaborator_list", **kwargs)


@collaborator.command()
@ident_filter("project", required=True)
@click.argument("userid")
@global_options
def info(**kwargs):
    """Retrieve the record of a single collaborator.

    The PROJECT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.

    USERID must be an exact match of the user ID of an individual, or the name
    of a group (e.g., 'everyone').
    """
    cluster_call("project_collaborator_info", **kwargs)


@collaborator.command()
@ident_filter("project", required=True)
@click.argument("userid", nargs=-1)
@click.option("--group", is_flag=True, help="The collaborator is a group.")
@click.option("--read-only/--read-write", is_flag=True, help="The collaborator should be read-only/read-write (default).")
@global_options
def add(**kwargs):
    """Add/modify one or more collaborators for a project.

    The PROJECT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.

    Each USERID must be an exact match of the user ID of an individual, or the name
    of a group (e.g., 'everyone'). It is not an error if this matches an existing
    collaborator, so this can be used to change the read-only status.
    """
    cluster_call("project_collaborator_add", **kwargs)


@collaborator.command()
@ident_filter("project", required=True)
@click.argument("userid", nargs=-1)
@global_options
def remove(**kwargs):
    """Remove one or more collaborators for a project.

    The PROJECT identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one project.

    Each USERID must be an exact match of the user ID of an individual, or the name
    of a group (e.g., 'everyone'). If the user ID is not among the current list
    of collaborators, an error is raised.
    """
    cluster_call("project_collaborator_remove", **kwargs)
