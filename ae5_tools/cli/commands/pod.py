import sys
import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import format_options


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@format_options()
@login_options()
def pod():
    '''Commands related to the AE5 pods (sessions, deployments, runs).

    These commands require that either K8S deployment to be live on the platform,
    or the --k8s-ssh-user option be supplied with a valid username.
    '''
    pass


@pod.command()
@format_options()
@login_options()
def list():
    '''List all nodes.'''
    cluster_call('pod_list', cli=True)


@pod.command()
@click.argument('pod')
@format_options()
@login_options()
def info(pod):
    '''Get information about a specific pod.'''
    cluster_call('pod_info', node, cli=True)
