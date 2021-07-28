import click

from ..login import cluster_call
from ..utils import global_options, yes_option


@click.group(short_help='list, add, delete',
             epilog='Type "ae5 secret <command> --help" for help on a specific command.')
@global_options
def secret():
    '''Commands related to static endpoints.'''
    pass


@secret.command()
def list(**kwargs):
    '''List the static endpoints on this cluster.
    '''
    cluster_call('secret_list', **kwargs)


@secret.command()
@click.argument('key')
@yes_option
@global_options
def delete(key, **kwargs):
    '''Delete a secret.

       The secret key must match exactly.
    '''
    cluster_call('secret_delete', key=key, **kwargs,
                 confirm=f'Delete secret {key}',
                 prefix=f'Deleting secret {key}...',
                 postfix='deleted.')


@secret.command()
@click.argument('key', type=str)
@click.argument('value')
@yes_option
@global_options
def add(key, value, **kwargs):
    '''Add a secret.

       Must provide key and value as arguments.
    '''
    cluster_call('secret_add', key=key, value=value, **kwargs,
                 confirm=f'Adding secret {key}',
                 prefix=f'Adding secret {key}...',
                 postfix='added.')
