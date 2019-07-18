import click

from ..utils import add_param
from ..login import login_options, cluster_call
from ..format import print_output, format_options


@click.group(short_help='List, add, or remove collaborators.',
             epilog='Type "ae5 deployment collaborator <command> --help" for help on a specific command.')
@format_options()
@login_options()
def collaborator():
    pass


@collaborator.command()
@click.argument('deployment')
@format_options()
@login_options()
def list(deployment):
    '''List the collaborators on a deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.
    '''
    result = cluster_call('deployment_collaborator_list', deployment, format='dataframe')
    print_output(result)


@collaborator.command()
@click.argument('deployment')
@click.argument('userid')
@format_options()
@login_options()
def info(deployment, userid):
    '''Retrieve the record of a single collaborator.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.

       USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone').
    '''
    result = cluster_call('deployment_collaborator_info', deployment, userid, format='dataframe')
    print_output(result)


@collaborator.command()
@click.argument('deployment')
@click.argument('userid', nargs=-1)
@click.option('--group', is_flag=True, help='The collaborator is a group.')
@format_options()
@login_options()
def add(deployment, userid, group):
    '''Add/modify one or more collaborators for a deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.

       Each USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone'). It is not an error if this matches an existing
       collaborator, so this can be used to change the read-only status.
    '''
    result = cluster_call('deployment_collaborator_add', deployment, userid, group, format='dataframe')
    print_output(result)


@collaborator.command()
@click.argument('deployment')
@click.argument('userid', nargs=-1)
@format_options()
@login_options()
def remove(deployment, userid):
    '''Remove one or more collaborators for a deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.

       Each USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone'). If the user ID is not among the current list
       of collaborators, an error is raised.
    '''
    result = cluster_call('deployment_collaborator_remove', deployment, userid, format='dataframe')
    print_output(result)