import click

from ..login import cluster_call
from ..utils import global_options


@click.group(short_help='Subcommands: add, info, list, remove',
             epilog='Type "ae5 deployment collaborator <command> --help" for help on a specific command.')
@global_options
def collaborator():
    '''Commands related to collaborators on a deployment.

       Since deployments are read-only, collaborators are simply those
       users or groups who can access the deployment.
    '''
    pass


@collaborator.command()
@click.argument('deployment')
@global_options
def list(deployment):
    '''List the collaborators on a deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.
    '''
    cluster_call('deployment_collaborator_list', deployment)


@collaborator.command()
@click.argument('deployment')
@click.argument('userid')
@global_options
def info(deployment, userid):
    '''Retrieve the record of a single collaborator.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.

       USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone').
    '''
    cluster_call('deployment_collaborator_info', deployment, userid)


@collaborator.command()
@click.argument('deployment')
@click.argument('userid', nargs=-1)
@click.option('--group', is_flag=True, help='The collaborator is a group.')
@global_options
def add(deployment, userid, group):
    '''Add/modify one or more collaborators for a deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.

       Each USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone'). It is not an error if this matches an existing
       collaborator, so this can be used to change the read-only status.
    '''
    cluster_call('deployment_collaborator_add', deployment, userid, group)


@collaborator.command()
@click.argument('deployment')
@click.argument('userid', nargs=-1)
@global_options
def remove(deployment, userid):
    '''Remove one or more collaborators for a deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.

       Each USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone'). If the user ID is not among the current list
       of collaborators, an error is raised.
    '''
    cluster_call('deployment_collaborator_remove', deployment, userid)
