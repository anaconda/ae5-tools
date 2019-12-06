import click

from ..utils import ident_filter, global_options
from ..login import cluster_call


@click.group(short_help='info, list',
             epilog='Type "ae5 editor <command> --help" for help on a specific command.')
@global_options
def editor():
    '''Commands related to development editors.'''
    pass


@editor.command()
@ident_filter('editor', 'id={value}|name={value}')
@global_options
def list():
    '''List the available editors.
    '''
    cluster_call('editor_list', cli=True)


@editor.command()
@click.argument('name')
@global_options
def info(name):
    '''Retrieve the record of a single editor.

       The NAME identifier must match exactly one name of an editor.
       Wildcards may be included.
    '''
    cluster_call('editor_info', name, cli=True)
