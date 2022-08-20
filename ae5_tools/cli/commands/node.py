import sys
import click

from ..login import cluster_call
from ..utils import global_options, ident_filter


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@global_options
def node():
    '''Commands related to the AE5 nodes.

    These commands require a live K8S deployment running on the
    platform, or the use of the --k8s-ssh-user option with a valid
    username.
    '''
    pass


@node.command()
@ident_filter('node')
@global_options
def list(**kwargs):
    '''List all nodes.'''
    cluster_call('node_list', **kwargs)


@node.command()
@ident_filter('node', required=True)
@global_options
def info(**kwargs):
    '''Get information about a specific node.'''
    cluster_call('node_info', **kwargs)
