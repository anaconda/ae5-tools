import click

from ..login import login_options, cluster_call
from ..format import print_output, format_options


@click.group(short_help='Subcommands: add, info, list, remove',
             epilog='Type "ae5 project collaborator <command> --help" for help on a specific command.')
@format_options()
@login_options()
def collaborator():
    '''Commands related to the collaborators on a project.'''
    pass


@collaborator.command()
@click.argument('project')
@format_options()
@login_options()
def list(project):
    '''List the collaborators on a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_collaborator_list', project, format='dataframe')
    print_output(result)


@collaborator.command()
@click.argument('project')
@click.argument('userid')
@format_options()
@login_options()
def info(project, userid):
    '''Retrieve the record of a single collaborator.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone').
    '''
    result = cluster_call('project_collaborator_info', project, userid, format='dataframe')
    print_output(result)


@collaborator.command()
@click.argument('project')
@click.argument('userid', nargs=-1)
@click.option('--group', is_flag=True, help='The collaborator is a group.')
@click.option('--read-only', is_flag=True, help='The collaborator should be read-only.')
@click.option('--read-write', is_flag=True, help='The collaborator should be read-write (default).')
@format_options()
@login_options()
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
    result = cluster_call('project_collaborator_add', project, userid, group, read_only, format='dataframe')
    print_output(result)


@collaborator.command()
@click.argument('project')
@click.argument('userid', nargs=-1)
@format_options()
@login_options()
def remove(project, userid):
    '''Remove one or more collaborators for a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       Each USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone'). If the user ID is not among the current list
       of collaborators, an error is raised.
    '''
    result = cluster_call('project_collaborator_remove', project, userid, format='dataframe')
    print_output(result)
