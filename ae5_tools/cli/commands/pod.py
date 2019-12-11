import sys
import click

from ..login import cluster_call
from ..utils import add_param, ident_filter, global_options


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@global_options
def pod():
    '''Commands related to the AE5 pods (sessions, deployments, runs).

    These commands require that either K8S deployment to be live on the platform,
    or the --k8s-ssh-user option be supplied with a valid username.
    '''
    pass


@pod.command()
@ident_filter('pod')
@global_options
def list(**kwargs):
    '''List all nodes.'''
    cluster_call('pod_list', **kwargs)


@pod.command()
@ident_filter('pod', required=True)
@global_options
def info(**kwargs):
    '''Get information about a specific pod.'''
    cluster_call('pod_info', **kwargs)
