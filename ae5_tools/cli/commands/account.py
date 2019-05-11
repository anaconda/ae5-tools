import click
import pandas as pd

from ..utils import add_param
from ..login import login_options, cluster
from ..format import print_output, format_options
from ...config import config


@click.group()
@click.pass_context
def account(ctx):
    pass


@account.command()
@format_options()
def list():
    h, u, p, t = [], [], [], []
    for hh, uu, pp, tt in config.list():
        h.append(hh)
        u.append(uu)
        p.append("Yes" if pp else "No")
        t.append(tt)
    result = pd.DataFrame({'hostname': h, 'username': u, 'password saved': p, 'session expires': t})
    print_output(result)


@account.command()
@login_options()
@click.option('--default', default=False, is_flag=False, help='Make the default.')
@click.option('--replace', default=False, is_flag=True, help='Do not ask for confirmation to replace.')
@click.pass_context
def add(ctx, default, replace):
    obj = ctx.ensure_object(dict)
    hostname = obj.get('hostname')
    username = obj.get('username')
    password = obj.get('password')
    if not hostname:
        hostname = my_input('Hostname', required=True, hidden=False)
    if not username:
        username = my_input('Username', required=True, hidden=False)
    key = f'{username}@{hostname}'
    if key in config._data:
        if not replace:
            replace = click.confirm(f'Replace existing entry for {username}@{hostname}')
            if not replace:
                return
        default = default or next(iter(config._data)) == key
    else:
        default = default or len(config._data) > 0
    if not password:
        password = my_input('Password (leave blank to prompt every time)', required=False, hidden=True)
    if not default:
        default = click.confirm(f'Make default', default=True)
    print(f'Connecting to {key}...')
    blank_password = not password
    if blank_password:
        password = my_input('Password (will not be saved)', default=obj.get('password', ''), required=True, hidden=True)
    add_param('hostname', hostname)
    add_param('username', username)
    add_param('password', password)
    c = cluster(reconnect=True)
    config.store(hostname, username, password, c.token, default)
    pstat = 'NOT ' if blank_password else ''
    print(f'Credential successfully stored; password {pstat}included.')


@account.command()
@login_options(password=False)
@click.option('--all', default=False, is_flag=True, help='Remove all saved credentials.')
@click.option('--yes', default=False, is_flag=True, help='Do not ask for confirmation.')
@click.pass_context
def remove(ctx, all, yes):
    obj = ctx.ensure_object(dict)
    hostname = obj.get('hostname')
    username = obj.get('username')
    if not all and not (hostname or username):
        hostname = my_input('Hostname (leave blank for "all")', required=False, hidden=False)
        username = my_input('Username (leave blank for "all")', required=False, hidden=False)
    to_remove = []
    for k in config._data:
        u, h = k.rsplit('@', 1)
        if (not hostname or h == hostname) and (not username or u == username):
            to_remove.append(k)
    if not to_remove:
        msg = 'No credentials'
        if hostname or username:
            msg += f' matching {username or "*"}@{hostname or "*"}'
        msg += ' found to remove.'
        return
    n = len(to_remove)
    ns = 's' if n > 1 else ''
    print(to_remove)
    click.echo(f'Removing {n} credential{ns}:\n  - ' + '\n  - '.join(to_remove))
    if yes or click.confirm('Proceed?'):
        config._data = {k: v for k, v in config._data.items() if k not in to_remove}
        config.write()
