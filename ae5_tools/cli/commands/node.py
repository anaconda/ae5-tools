import sys
import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import format_options


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@format_options()
@login_options()
def node():
    '''Commands related to the AE5 nodes.

    These commands require that either K8S deployment to be live on the platform,
    or the --k8s-ssh-user option be supplied with a valid username.
    '''
    pass


@node.command()
@format_options()
@login_options()
def list():
    '''List all nodes.'''
    cluster_call('node_list', cli=True)


@node.command()
@click.argument('node')
@format_options()
@login_options()
def info(node):
    '''Get information about a specific node.'''
    cluster_call('node_info', node, cli=True)
