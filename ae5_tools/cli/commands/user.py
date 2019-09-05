import sys
import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import format_options


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@format_options()
@login_options()
def user():
    '''Commands related to user accounts.

    These commands require the use of the KeyCloak administrator account.
    '''
    pass


@user.command()
@click.argument('username', required=False)
@format_options()
@login_options()
def list(username):
    '''List all users.'''
    if username:
        add_param('filter', f'username={username}')
    cluster_call('user_list', cli=True, admin=True)


@user.command()
@click.argument('username')
@format_options()
@login_options()
def info(username):
    '''Retrieve information about a single user.

    USERNAME must exactly match either one username or one KeyCloak user ID.'''
    cluster_call('user_info', username, cli=True, admin=True)


@user.command()
@click.argument('param', nargs=-1)
@click.option('--limit', type=click.IntRange(1), default=sys.maxsize, help='The maximum number of events to return.')
@click.option('--first', type=click.IntRange(0), default=0, help='The index of the first element to return.')
@format_options()
@login_options()
def events(param, limit, first):
    '''Retrieve KeyCloak events.

    Each PARAM argument must be of the form <key>=<value>.
    '''
    param = [z.split('=', 1) for z in param]
    param = dict((x.rstrip(), y.lstrip()) for x, y in param)
    cluster_call('user_events', limit=limit, first=first, **param, cli=True, admin=True)
