import os
import re
import sys
import json
import click

from fnmatch import fnmatch
from datetime import datetime

from .utils import param_callback, click_text, get_options

IS_WIN = sys.platform.startswith('win')


def print_format_help(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click_text('''
@Formatting the tabular output: options

Many AE5 commands provide output in JSON tabular form---either a single
dictionary or a list of dictionaries with identical keys. By default, the tables
are rendered in text suitable for viewing in a terminal. The output can be modified
in a variety of ways with the following options. Non-tabular output is always
returned in plain text form.

In REPL mode, formatting options supplied on the command line serve as the default
values for all commands executed in that session, but can be overridden on a
per-command basis.

@Options:
''')
    for option, help in _format_help.items():
        text = f'--{option}'
        spacer = ' ' * (13 - len(text))
        text = f'{text}{spacer}{help}'
        click.echo(click.wrap_text(text, initial_indent='  ', subsequent_indent=' ' * 15))
    ctx.exit()


def print_filter_help(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click_text('''
@Filtering the rows of tabular output

The argument of the --filter argument accepts a set of simple filter expressions
combined using AND or OR relationships.

Single filters may take the form <field> <op> <value>,
where <op> is a binary comparison operator:
    - =, ==, !=, <, <=, >, >=, =
The field name must exactly match a column of the table.
Whitespace on either side of the operator is ignored. The single
equals sign accepts wildcard values, matched using fnmatch.
All other operators perform standard string comparison.

AND combinations can be separated by either ampersands or commas:
    - <filter1>&<filter2>&...&<filterN>
    - <filter1>,<filter2>,...,<filterN>
OR combinations can be separated by pipes:
    - <filter1>|<filter2>|...|<filterN>
Because of the interpretation many shells place on ampersands and vertical bars,
the use of quotes to surround the composite expression is strongly encouraged.

As with standard logical operations in most programming languages, the ampersand
has higher precedence than the pipe; for instance,
    --filter <filter1>&<filter2>|<filter3>
is interpreted as (<filter1> AND <filter2>) OR <filter3>. However, the comma has
lower precedence than the pipe; for instance,
    --filter <filter1>,<filter2>|<filter3>
is interpreted as <filter1> AND (<filter2> OR <filter3>).
''')
    ctx.exit()


_format_help = {
    'format': 'Output format: "text" (default), "csv", and "json".',
    'filter': 'Filter the rows with a comma-separated list of <field>=<value> pairs. Use the --help-filter option for more information on how to construct filter operations.',
    'columns': 'Limit the output to a comma-separated list of columns.',
    'sort': 'Sort the rows by a comma-separated list of fields.',
    'width': 'Output width, in characters. The default behavior is to determine the width of the surrounding window and truncate the table to that width. Only applies to the "text" format.',
    'wide': 'Do not limit output width. Equivalent to --width=infinity.',
    'no-header': 'Omit the header. Applies to "text" and "csv" formats only.'
}


_format_options = [
    click.option('--filter', type=str, default=None, expose_value=False, callback=param_callback, hidden=True, multiple=True),
    click.option('--columns', type=str, default=None, expose_value=False, callback=param_callback, hidden=True),
    click.option('--sort', type=str, default=None, expose_value=False, callback=param_callback, hidden=True),
    click.option('--format', type=click.Choice(['text', 'csv', 'json']), default=None, expose_value=False, callback=param_callback, hidden=True),
    click.option('--width', type=int, default=None, expose_value=False, callback=param_callback, hidden=True),
    click.option('--wide', is_flag=True, default=None, expose_value=False, callback=param_callback, hidden=True),
    click.option('--header/--no-header', default=None, expose_value=False, callback=param_callback, hidden=True),
    click.option('--help-format', is_flag=True, default=None, callback=print_format_help, expose_value=False, is_eager=True,
                 help='Get help on the output formatting options.'),
    click.option('--help-filter', is_flag=True, default=None, callback=print_filter_help, expose_value=False, is_eager=True,
                 help='Get help on the row filtering options.')
]


def format_options():
    def apply(func):
        for option in reversed(_format_options):
            func = option(func)
        return func
    return apply


OPS = {'<' : lambda x, y: x < y,
       '<=': lambda x, y: x <= y,
       '>' : lambda x, y: x > y,
       '>=': lambda x, y: x >= y,
        '=': lambda x, y: fnmatch(x, y),
       '==': lambda x, y: x == y,
       '!=': lambda x, y: not fnmatch(x, y)}


def filter_df(records, df_columns, filter, columns=None):
    if not records:
        return records, df_columns
    if columns:
        columns = columns.split(',')
        missing = '\n  - '.join(set(columns) - set(df_columns))
        if missing:
            raise click.UsageError(f'One or more of the requested columns were not found:\n  - {missing}')
    mask0 = None
    for filt1 in filter or ():
        mask1 = None
        for filt2 in filt1.split(','):
            mask2 = None
            for filt3 in filt2.split('|'):
                mask3 = None
                for filt4 in filt3.split('&'):
                    parts = re.split(r'(==?|!=|>=?|<=?)', filt4.strip())
                    if len(parts) != 3:
                        raise click.UsageError(f'Invalid filter string: {filt4}\n   Required format: <fieldname><op><value>')
                    field, op, value = list(map(str.strip, parts))
                    if field not in df_columns:
                        raise click.UsageError(f'Invalid filter field: {field}')
                    op = OPS[op]
                    mask4 = [op(str(rec.get(field,'')), value) for rec in records]
                    mask3 = mask4 if mask3 is None else [m1 and m2 for m1, m2 in zip(mask3, mask4)]
                mask2 = mask3 if mask2 is None else [m1 or m2 for m1, m2 in zip(mask2, mask3)]
            mask1 = mask2 if mask1 is None else [m1 and m2 for m1, m2 in zip(mask1, mask2)]
        mask0 = mask1 if mask0 is None else [m1 and m2 for m1, m2 in zip(mask0, mask1)]
    if mask0:
        records = [rec for rec, flag in zip(records, mask0) if flag]
    if columns and records:
        records = [{key: rec[key] for key in columns if key in rec} for rec in records]
        df_columns = columns
    return records, df_columns


def sort_df(records, columns):
    if not records or not columns:
        return records
    ndx0 = ndxs = list(range(len(records)))
    for col in columns.split(',')[::-1]:
        desc = col.startswith('-')
        if desc:
            col = col[1:]
        ndx2 = sorted(ndx0, key=lambda x: records[ndxs[x]].get(col, ''))
        if desc:
            ndx2 = ndx2[::-1]
        ndxs = [ndxs[x] for x in ndxs2]
    return [rec[x] for x in ndxs]


def _str(x, isodate=False):
    if isinstance(x, datetime):
        if isodate:
            return x.isoformat()
        return x.strftime("%m-%d-%Y %H:%M:%S")
    return str(x)


def json_datetime(o):
    if isinstance(o, datetime):
        return o.isoformat()


def print_df(records, columns, header=True, width=0):
    if width <= 0:
        # http://granitosaurus.rocks/getting-terminal-size.html
        for i in range(3):
            try:
                width = int(os.get_terminal_size(i)[0]) - IS_WIN
                break
            except OSError:
                pass
        else:
            width = 80
    nwidth = -2
    records = [{str(k): _str(v) for k, v in rec.items()} for rec in records]
    for col in columns:
        col = str(col)
        val = [rec.get(col, '') for rec in records]
        twid = max(len(col), max((len(v) for v in val), default=len(col)))
        val = [v + ' ' * (twid - len(v)) for v in val]
        if len(col) > twid:
            col = col[:twid-1] + '.'
        col = col + ' ' * (twid - len(col))
        if nwidth < 0:
            final = val
            head = col
            dash = '-' * twid
        else:
            final = [x + '  ' + y for x, y in zip(final, val)]
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
    if result is None:
        return
    elif isinstance(result, str):
        if result:
            print(result)
        return
    elif isinstance(result, (list, dict)):
        print(json.dumps(result, indent=2, default=json_datetime))
        return
    elif not isinstance(result, tuple):
        raise NotImplementedError(f'Not prepared to print an object of type {type(result)}')
    result, columns = result
    is_series = isinstance(result, dict)
    if is_series:
        result = [result]
    opts = get_options()
    if opts.get('filter') or opts.get('columns'):
        result, columns = filter_df(result, columns, opts.get('filter'), opts.get('columns'))
    if not is_series and 'sort' in opts:
        result = sort_df(result, opts.get('sort'))
    if is_series:
        result = result[0]
    fmt = opts.get('format')
    if fmt == 'json':
        print(json.dumps(result, indent=2, default=json_datetime))
        return
    if isinstance(result, dict):
        columns = ['field', 'value']
        result = [{'field': k, 'value': v} for k, v in result[0].items()]
    if fmt == 'csv':
        if opts.get('header', True):
            print(','.join(columns))
        for rec in result:
            print(','.join(_str(rec[col], True) for col in columns))
    else:
        width = 99999999 if opts.get('wide') else opts.get('width') or 0
        print_df(result, columns, opts.get('header', True), width)
