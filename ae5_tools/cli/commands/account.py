import click
import pandas as pd

from ..format import print_output, format_options
from ..login import login_options
from ...config import config


@click.group(short_help='list',
             epilog='Type "ae5 account <command> --help" for help on a specific command.')
@format_options()
@login_options()
@click.pass_context
def account(ctx):
    '''list'''
    pass


@account.command()
@format_options()
def list():
    h, u, a, l, t = [], [], [], [], []
    for hh, uu, aa, ll, tt in config.list():
        h.append(hh)
        u.append(uu)
        a.append("Yes" if aa else "No")
        l.append(ll)
        t.append(tt)
    result = pd.DataFrame({'hostname': h, 'username': u, 'admin': a,
                           'last used': l, 'session expires': t})
    print_output(result)
