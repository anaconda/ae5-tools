import os
import json
import click
import pandas as pd

from fnmatch import fnmatch

from .utils import param_callback


_format_options = [
    click.option('--filter', type=str, expose_value=False, callback=param_callback,
                 help='Filter the rows with a comma-separated list of <field>=<value> pairs. Wildcards may be used in the values.'),
    click.option('--columns', type=str, expose_value=False, callback=param_callback,
                 help='Limit output to a comma-separated list of columns.'),
    click.option('--sort', type=str, expose_value=False, callback=param_callback,
                 help='Sort the rows by a comma-separated list of fields.'),
    click.option('--format', type=click.Choice(['text', 'csv', 'json']), expose_value=False, callback=param_callback,
                 help='Output format. Default is "text".'),
    click.option('--width', type=int, expose_value=False, callback=param_callback,
                 help='Output width, in characters (format="text" only). Default is to limit to width of the window.'),
    click.option('--wide', is_flag=True, expose_value=False, callback=param_callback,
                 help='Do not limit output width (format="text" only). Equivalent to --width=infinity.'),
    click.option('--header/--no-header', default=True, expose_value=False, callback=param_callback,
                 help='Include header (format="text"/"csv" only).')
]


def format_options():
    def apply(func):
        for option in reversed(_format_options):
            func = option(func)
        return func
    return apply


def filter_df(df, filter_string, columns=None):
    if columns:
        columns = columns.split(',')
        missing = '\n  - '.join(set(columns) - set(df.columns))
        if missing:
            raise click.UsageError(f'One or more of the selected columns were not found:\n  - {missing}')
    if filter_string not in (None, ''):
        filters = filter_string.split(',')
        for filter in filters:
            if '=' not in filter:
                raise click.UsageError(f'Invalid filter string: {filter}\n   Required format: <fieldname>=<value> or <fieldname>!=<value>')
            field, value = filter.split('=', 1)
            mask = [fnmatch(str(row), value) for row in df[field.rstrip('!')]]
            if field.endswith('!'):
                mask = [not m for m in mask]
            df = df[mask]
    if columns:
        df = df[columns]
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
        twid = max(len(col), val.str.len().max())
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
                head = head[:width] if head[width - n:width] == s else head[:width - n] + d
                dash = dash[:width]
                final = [f[:width] if f[width - n:width] == s else f[:width - n] + d
                         for f in final]
            break
    if header:
        print(head.rstrip())
        print(dash)
    if len(final):
        print('\n'.join(map(str.rstrip, final)))


def print_output(result):
    obj = click.get_current_context().find_object(dict)
    is_single = isinstance(result, pd.Series)
    if is_single:
        result = result.T.reset_index()
        result.columns = ['field', 'value']
    if obj.get('filter') or obj.get('columns'):
        result = filter_df(result, obj.get('filter'), obj.get('columns'))
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
            print(result.columns)
        result = json.loads(result.to_json(orient=orient, date_format='iso'))
        print(json.dumps(result, indent=2))
    else:
        width = 99999999 if obj.get('wide') else obj.get('width') or 0
        print_df(result, obj.get('header', True), width)
