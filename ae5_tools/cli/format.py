import os
import json
import click
import pandas as pd

from fnmatch import fnmatch

from .utils import param_callback


def print_format_help(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('Formatting the tabular output: options\n')
    click.echo(click.wrap_text((
'Most of the CLI commands provide output in tabular form. By default, the tables '
'are rendered in text suitable for viewing in a terminal. The output can be modified '
'in a variety of ways with the following options.'), initial_indent='  ', subsequent_indent='  '))
    click.echo('\nOptions:')
    for option, help in _format_help.items():
        text = f'--{option}'
        spacer = ' ' * (13 - len(text))
        text = f'{text}{spacer}{help}'
        click.echo(click.wrap_text(text, initial_indent='  ', subsequent_indent=' ' * 15))
    ctx.exit()


_format_help = {
    'format': 'Output format: "text" (default), "csv", and "json".',
    'filter': 'Filter the rows with a comma-separated list of <field>=<value> pairs. Wildcards may be used in the values.',
    'columns': 'Limit the output to a comma-separated list of columns.',
    'sort': 'Sort the rows by a comma-separated list of fields.',
    'width': 'Output width, in characters. The default behavior is to determine the width of the surrounding window and truncate the table to that width. Only applies to the "text" format.',
    'wide': 'Do not limit output width. Equivalent to --width=infinity.',
    'no-header': 'Omit the header. Applies to "text" and "csv" formats only.'
}


_format_options = [
    click.option('--filter', type=str, expose_value=False, callback=param_callback, hidden=True),
    click.option('--columns', type=str, expose_value=False, callback=param_callback, hidden=True),
    click.option('--sort', type=str, expose_value=False, callback=param_callback, hidden=True),
    click.option('--format', type=click.Choice(['text', 'csv', 'json']), expose_value=False, callback=param_callback, hidden=True),
    click.option('--width', type=int, expose_value=False, callback=param_callback, hidden=True),
    click.option('--wide', is_flag=True, expose_value=False, callback=param_callback, hidden=True),
    click.option('--header/--no-header', default=True, expose_value=False, callback=param_callback, hidden=True),
    click.option('--help-format', is_flag=True, callback=print_format_help, expose_value=False, is_eager=True,
                 help='Get help on the output formatting options.')
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
    if not columns:
        for ndx in range(len(df.columns)):
            if len(df.iloc[:,:ndx+1].drop_duplicates()) == len(df):
                columns = list(df.columns[:ndx+1])
                break
        else:
            return df
        ascending = [True] * len(columns)
    else:
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
    if obj.get('format') != 'json':
        result = result.applymap(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else str(x))
    if obj.get('filter') or obj.get('columns'):
        result = filter_df(result, obj.get('filter'), obj.get('columns'))
    if not is_single and 'sort' in obj:
        result = sort_df(result, obj.get('sort'))
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
