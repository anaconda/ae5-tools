import os
import re
import json
import click
import pandas as pd

from fnmatch import fnmatch
from collections import namedtuple

from ..config import config
from ..api import AECluster


def add_format_param(param, value):
    if value is None:
        return
    ctx = click.get_current_context()
    ctx.ensure_object(dict)
    frec = ctx.obj.get('format')
    if frec is None:
        ctx.obj['format'] = frec = {
            'filter': '',
            'sort': '',
            'format': 'text',
            'width': 0,
            'wide': False,
            'header': True,
        }
    if param == 'filter':
        ovalue = frec['filter']
        value = f'{ovalue},{value}' if ovalue and value else (value or ovalue)
    print(param, value)
    frec[param] = value


def format_callback(ctx, param, value):
    add_format_param(param.name, value)


_format_options = [
    click.option('--filter', expose_value=False, callback=format_callback, help='Filter the rows using a comma-separated list of <field>=<value> pairs. Wildcards may be used.'),
    click.option('--sort', expose_value=False, callback=format_callback, help='Sort the rows by a comma-separated list of fields.'),
    click.option('--format', type=click.Choice(['text', 'csv', 'json']), expose_value=False, callback=format_callback, help='Output format. Default is "text".'),
    click.option('--width', expose_value=False, callback=format_callback, help='Output width, in characters (format="text" only). Default is to limit to width of the window'),
    click.option('--wide', is_flag=True, expose_value=False, callback=format_callback, help='Do not limit output width (format="text" only). Equivalent to --width=infinity.'),
    click.option('--header/--no-header', is_flag=True, default=True, expose_value=False, callback=format_callback, help='Include header (format="text"/"csv" only)')
]


def cluster():
    ctx = click.get_current_context()
    ctx.ensure_object(dict)
    if 'cluster' not in ctx.obj:
        hostname, username, password = config.find()
        if ctx.obj.get('in_repl'):
            click.echo(f'Connecting to {hostname} as user {username}...')
        ctx.obj['cluster'] = AECluster(hostname, username, password)
    return ctx.obj['cluster']


def format_options(single=False):
    def apply(func):
        for option in reversed(_format_options):
            func = option(func)
        return func
    return apply


class Identifier(namedtuple('Identifier', ['name', 'owner', 'id', 'revision'])):
    @classmethod
    def from_string(self, idstr):
        rev_parts = idstr.rsplit(':', 1)
        revision = rev_parts[1] if len(rev_parts) == 2 else ''
        id_parts = rev_parts[0].split('/')
        id, owner, name = '', '', ''
        if re.match(r'[a-f0-9]{2}-[a-f0-9]{32}', id_parts[-1]):
            id = id_parts.pop()
        if id_parts:
            name = id_parts.pop()
        if id_parts:
            owner = id_parts.pop()
        if id_parts:
            raise ValueError(f'Invalid identifier: {idstr}')
        return Identifier(name, owner, id, revision)

    def project_filter(self):
        parts = []
        if self.name and self.name != '*':
            parts.append(f'name={self.name}')
        if self.owner and self.owner != '*':
            parts.append(f'owner={self.owner}')
        if self.id and self.id != '*':
            parts.append(f'id={self.id}')
        if parts:
            return ','.join(parts)

    def revision_filter(self):
        if self.revision and self.revision != '*':
            return f'name={self.revision}'

    def to_string(self, drop_revision=False):
        if self.id:
            if self.owner or self.name:
                result = f'{self.owner or "*"}/{self.name or "*"}/{self.id}'
            else:
                result = self.id
        elif self.owner:
            result = f'{self.owner}/{self.name or "*"}'
        else:
            result = self.name
        if self.revision and not drop_revision:
            result = f'{result}:{self.revision}'
        return result


def filter_df(df, filter_string, is_revision=False):
    if filter_string in (None, ''):
        return df
    if '=' not in filter_string:
        filter_string = Identifier.from_string(filter_string)
    if isinstance(filter_string, Identifier):
        if is_revision:
            filter_string = filter_string.revision_filter()
        else:
            filter_string = filter_string.project_filter()
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


def print_df(df, header=True, width=None):
    if not width:
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
    print('\n'.join(final))


def print_output(result):
    frec = click.get_current_context().ensure_object(dict)['format']
    print(frec)
    is_single = isinstance(result, pd.Series)
    if is_single:
        result = result.T.reset_index()
        result.columns = ['field', 'value']
    if frec['filter']:
        result = filter_df(result, frec['filter'], is_single)
    if frec['sort']:
        result = sort_df(result, frec['sort'])
    if frec['format'] == 'csv':
        print(result.to_csv(index=False, header=frec['header']))
    elif frec['format'] == 'json':
        if is_single:
            result = result.set_index('field').value
        orient = 'index' if is_single else 'records'
        result = json.loads(result.to_json(orient=orient, date_format='iso'))
        print(json.dumps(result, indent=2))
    else:
        width = 99999999 if frec['wide'] else frec['width']
        print_df(result, frec['header'], width)

