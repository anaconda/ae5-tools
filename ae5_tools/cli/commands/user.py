import sys
import click

from ..login import cluster_call
from ..utils import add_param, global_options, ident_filter


@click.group(short_help='info, list',
             epilog='Type "ae5 user <command> --help" for help on a specific command.')
@global_options
def user():
    '''Commands related to user accounts.

    These commands require the use of the KeyCloak administrator account.
    '''
    pass


@user.command()
@ident_filter('uname', 'username={value}|id={value}')
@global_options
def list():
    '''List all users.'''
    cluster_call('user_list', cli=True, admin=True)


@user.command()
@click.argument('uname')
@global_options
def info(uname):
    '''Retrieve information about a single user.

    UNAME must exactly match either one username or one KeyCloak user ID.'''
    cluster_call('user_info', uname, cli=True, admin=True)


@user.command()
@click.argument('param', nargs=-1)
@click.option('--limit', type=click.IntRange(1), default=sys.maxsize, help='The maximum number of events to return.')
@click.option('--first', type=click.IntRange(0), default=0, help='The index of the first element to return.')
@global_options
def events(param, limit, first):
    '''Retrieve KeyCloak events.

    Each PARAM argument must be of the form <key>=<value>.
    '''
    param = [z.split('=', 1) for z in param]
    param = dict((x.rstrip(), y.lstrip()) for x, y in param)
    cluster_call('user_events', limit=limit, first=first, **param, cli=True, admin=True)
