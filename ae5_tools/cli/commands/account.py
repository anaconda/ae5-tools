import click

from ...config import config
from ..format import print_output
from ..utils import global_options


@click.group(short_help="list", epilog='Type "ae5 account <command> --help" for help on a specific command.')
@global_options
def account():
    """Commands related to the saved session information."""
    pass


@account.command()
@global_options
def list():
    columns = ("hostname", "username", "admin", "last used", "session expires")
    records = config.list()
    print_output((records, columns))
