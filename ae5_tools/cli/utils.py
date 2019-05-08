import os
import re
import json
import click
import pandas as pd

from fnmatch import fnmatch
from collections import namedtuple

from ..config import config
from ..api import AECluster
from ..identifier import Identifier


def add_param(param, value):
    if value is None:
        return
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    if param == 'filter':
        ovalue = obj.get('filter') or ''
        value = f'{ovalue},{value}' if ovalue and value else (value or ovalue)
    obj[param] = value


def param_callback(ctx, param, value):
    add_param(param.name, value)


_format_options = [
    click.option('--filter', type=str, expose_value=False, callback=param_callback,
                 help='Filter the rows using a comma-separated list of <field>=<value> pairs. Wildcards may be used.'),
    click.option('--sort', type=str, expose_value=False, callback=param_callback,
                 help='Sort the rows by a comma-separated list of fields.'),
    click.option('--format', type=click.Choice(['text', 'csv', 'json']), expose_value=False, callback=param_callback,
                 help='Output format. Default is "text".'),
    click.option('--width', type=int, expose_value=False, callback=param_callback,
                 help='Output width, in characters (format="text" only). Default is to limit to width of the window'),
    click.option('--wide', is_flag=True, expose_value=False, callback=param_callback,
                 help='Do not limit output width (format="text" only). Equivalent to --width=infinity.'),
    click.option('--header/--no-header', default=True, expose_value=False, callback=param_callback,
                 help='Include header (format="text"/"csv" only)')
]


_login_options = [
    click.option('--hostname', type=str, expose_value=False, callback=param_callback, envvar='AE5_HOSTNAME',
                 help='The hostname of the cluster to connect to.'),
    click.option('--username', type=str, expose_value=False, callback=param_callback, envvar='AE5_USERNAME',
                 help='The username to use for authentication.'),
    click.option('--password', type=str, expose_value=False, callback=param_callback, envvar='AE5_PASSWORD',
                 help='The password to use for authentication.')
]


def cluster(reconnect=False):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    if 'cluster' not in obj or reconnect:
        hostname = obj.get('hostname')
        username = obj.get('username')
        password = obj.get('password')
        if not hostname and not username:
            matches = config.default()
            matches = [matches] if matches else []
        else:
            matches = config.resolve(hostname, username)
        if len(matches) == 1:
            hostname, username = matches[0]
        else:
            ask = obj.get('is_interactive')
            if hostname or username:
                if len(matches) == 0:
                    msg = 'No saved sessions match'
                else:
                    msg = 'Multiple saved accounts match'
                if hostname:
                    msg += f' hostname "{hostname}"'
                if hostname and username:
                    msg += ' and'
                if username:
                    msg += f' username "{username}"'
                if obj.get('is_interactive'):
                    click.echo(msg)
                else:
                    click.UsageError(msg)
            elif not ask:
                click.UsageError('Must supply username and hostname')
            hostname = click.prompt('Hostname', default=hostname, type=str)
            username = click.prompt('Username', default=username, type=str)
            password = click.prompt('Password', default=password, type=str, hide_input=True)
        obj['cluster'] = AECluster(hostname, username, password)
    return obj['cluster']


def format_options():
    def apply(func):
        for option in reversed(_format_options):
            func = option(func)
        return func
    return apply


def login_options(password=True):
    def apply(func):
        n = 3 if password else 2
        for option in reversed(_login_options[:n]):
            func = option(func)
        return func
    return apply


def filter_df(df, filter_string, is_revision=False):
    if filter_string in (None, ''):
        return df
    filters = filter_string.split(',')
    for filter in filters:
        if '=' not in filter:
            raise click.UsageError(f'Invalid filter string: {filter}\n   Required format: <fieldname>=<value>')
        field, value = filter.split('=', 1)
        df = df[[fnmatch(str(row), value) for row in df[field]]]
    return df


def sort_df(df, columns):
    columns = columns.split(',')
    ascending = [not c.startswith('-') for c in columns]
    columns = [c.lstrip('-') for c in columns]
    df = df.sort_values(by=columns, ascending=ascending)
    return df


def print_df(df, header=True, width=0):
    if width <= 0:
        # http://granitosaurus.rocks/getting-terminal-size.html
        for i in range(3):
            try:
                width = int(os.get_terminal_size(i)[0])
                break
            except OSError:
                pass
        else:
            width = 80
    nwidth = -2
    for col, val in df.items():
        col = str(col)
        val = val.astype(str)
        twid = max(len(col), val.str.len().max()) # if len(val) else len(col)
        val = val.str.pad(twid, 'right')
        col = col[:twid]
        col = col + ' ' * (twid - len(col))
        if nwidth < 0:
            final = val.values
            head = col
            dash = '-' * twid
        else:
            final = final + '  ' + val.values
            head = head + '  ' + col
            dash = dash + '  ' + '-' * twid
        owidth, nwidth = nwidth, nwidth + twid + 2
        if nwidth >= width:
            if nwidth > width:
                n = min(3, max(0, width - owidth - 2))
                d, s = '.' * n, ' ' * n
                head = head[:width] if head[width-n:width] == s else head[:width-n] + d
                dash = dash[:width]
                final = [f[:width] if f[width-n:width] == s else f[:width-n] + d
                         for f in final]
            break
    if header:
        print(head)
        print(dash)
    if len(final):
        print('\n'.join(final))


def print_output(result):
    obj = click.get_current_context().find_object(dict)
    is_single = isinstance(result, pd.Series)
    if is_single:
        result = result.T.reset_index()
        result.columns = ['field', 'value']
    if obj.get('filter'):
        result = filter_df(result, obj['filter'], is_single)
    if obj.get('sort'):
        result = sort_df(result, obj['sort'])
    if obj.get('format') == 'csv':
        print(result.to_csv(index=False, header=obj.get('header', True)))
    elif obj.get('format') == 'json':
        if is_single:
            result = result.set_index('field').value
            orient = 'index'
        else:
            orient = 'records'
        result = json.loads(result.to_json(orient=orient, date_format='iso'))
        print(json.dumps(result, indent=2))
    else:
        width = 99999999 if obj.get('wide') else obj.get('width') or 0
        print_df(result, obj.get('header', True), width)

