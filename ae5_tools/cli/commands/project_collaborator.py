import click

from ..login import cluster_call
from ..utils import global_options


@click.group(short_help='Subcommands: add, info, list, remove',
             epilog='Type "ae5 project collaborator <command> --help" for help on a specific command.')
@global_options
def collaborator():
    '''Commands related to the collaborators on a project.'''
    pass


@collaborator.command()
@click.argument('project')
@global_options
def list(project):
    '''List the collaborators on a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_collaborator_list', project, cli=True)


@collaborator.command()
@click.argument('project')
@click.argument('userid')
@global_options
def info(project, userid):
    '''Retrieve the record of a single collaborator.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone').
    '''
    cluster_call('project_collaborator_info', project, userid, cli=True)


@collaborator.command()
@click.argument('project')
@click.argument('userid', nargs=-1)
@click.option('--group', is_flag=True, help='The collaborator is a group.')
@click.option('--read-only', is_flag=True, help='The collaborator should be read-only.')
@click.option('--read-write', is_flag=True, help='The collaborator should be read-write (default).')
@global_options
def add(project, userid, group, read_only, read_write):
    '''Add/modify one or more collaborators for a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       Each USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone'). It is not an error if this matches an existing
       collaborator, so this can be used to change the read-only status.
    '''
    if read_only and read_write:
        raise click.ClickException('Cannot specify both --read-only and --read-write')
    cluster_call('project_collaborator_add', project, userid, group, read_only, cli=True)


@collaborator.command()
@click.argument('project')
@click.argument('userid', nargs=-1)
@global_options
def remove(project, userid):
    '''Remove one or more collaborators for a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       Each USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone'). If the user ID is not among the current list
       of collaborators, an error is raised.
    '''
    cluster_call('project_collaborator_remove', project, userid, cli=True)
