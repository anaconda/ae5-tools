import sys
import click

from ..login import cluster_call
from ..utils import add_param, global_options


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@global_options
def node():
    '''Commands related to the AE5 nodes.

    These commands require that either K8S deployment to be live on the platform,
    or the --k8s-ssh-user option be supplied with a valid username.
    '''
    pass


@node.command()
@global_options
def list():
    '''List all nodes.'''
    cluster_call('node_list', cli=True)


@node.command()
@click.argument('node')
@global_options
def info(node):
    '''Get information about a specific node.'''
    cluster_call('node_info', node, cli=True)
