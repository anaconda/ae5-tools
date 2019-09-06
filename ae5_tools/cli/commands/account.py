import click

from ..format import print_output, format_options
from ..login import login_options
from ...config import config


@click.group(short_help='list',
             epilog='Type "ae5 account <command> --help" for help on a specific command.')
@format_options()
@login_options()
@click.pass_context
def account(ctx):
    '''Commands related to the saved session information.'''
    pass


@account.command()
@format_options()
def list():
    columns = ('hostname', 'username', 'admin', 'last used', 'session expires')
    records = config.list()
    print_output((records, columns))
