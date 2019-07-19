import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import print_output, format_options


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@format_options()
@login_options()
def user():
    pass


@user.command()
@click.argument('username', required=False)
@format_options()
@login_options()
def list(username):
    result = cluster_call('user_list', format='dataframe', admin=True)
    if username:
        add_param('filter', f'username={username}')
    print_output(result)


@user.command()
@click.argument('username')
@format_options()
@login_options()
def info(username):
    result = cluster_call('user_info', username, format='dataframe', admin=True)
    print_output(result)
