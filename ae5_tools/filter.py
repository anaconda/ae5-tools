import re

from fnmatch import fnmatch
from datetime import datetime


def _str(x, isodate=False):
    if x is None:
        return ''
    elif isinstance(x, datetime):
        if isodate:
            return x.isoformat()
        return x.strftime("%m-%d-%Y %H:%M:%S")
    else:
        return str(x)


OPS = {'<': lambda x, y: x < y,
       '>': lambda x, y: x > y,
       '=': lambda x, y: fnmatch(x, y),
       '<=': lambda x, y: x <= y,
       '>=': lambda x, y: x >= y,
       '==': lambda x, y: x == y,
       '!=': lambda x, y: not fnmatch(x, y)}


def filter_vars(filter):
    vars = []
    if isinstance(filter, str):
        filter = filter,
    for filt1 in filter or ():
        for filt2 in filt1.split(','):
            for filt3 in filt2.split('|'):
                for filt4 in filt3.split('&'):
                    parts = re.split(r'(==?|!=|>=?|<=?)', filt4.strip())
                    if parts[0] not in vars:
                        vars.append(parts[0])
    return vars


def split_filter(filter, columns, negative=False):
    pre_filt = []
    post_filt = []
    if isinstance(filter, str):
        filter = filter,
    for filt1 in filter or ():
        for filt2 in filt1.split(','):
            for filt3 in filt2.split('|'):
                to_post = False
                for filt4 in filt3.split('&'):
                    parts = re.split(r'(==?|!=|>=?|<=?)', filt4.strip())
                    if (parts[0] in columns) == negative:
                        to_post = True
                        break
                if to_post:
                    break
            (post_filt if to_post else pre_filt).append(filt2)
    return pre_filt, post_filt


def filter_list_of_dicts(records, filter):
    if not filter or not records:
        return records
    mask0 = None
    rec0 = records[0]
    if isinstance(filter, str):
        filter = filter,
    for filt1 in filter or ():
        mask1 = None
        for filt2 in filt1.split(','):
            mask2 = None
            for filt3 in filt2.split('|'):
                mask3 = None
                for filt4 in filt3.split('&'):
                    parts = re.split(r'(==?|!=|>=?|<=?)', filt4.strip())
                    if len(parts) != 3:
                        raise ValueError(f'Invalid filter string: {filt4}\n   Required format: <fieldname><op><value>')
                    field, op, value = list(map(str.strip, parts))
                    if field not in rec0:
                        raise ValueError(f'Invalid filter string: unknown field "{field}"')
                    mask4 = [OPS[op](_str(rec[field]), value) if field in rec else False
                             for rec in records]
                    mask3 = mask4 if mask3 is None else [m1 and m2 for m1, m2 in zip(mask3, mask4)]
                mask2 = mask3 if mask2 is None else [m1 or m2 for m1, m2 in zip(mask2, mask3)]
            mask1 = mask2 if mask1 is None else [m1 and m2 for m1, m2 in zip(mask1, mask2)]
        mask0 = mask1 if mask0 is None else [m1 and m2 for m1, m2 in zip(mask0, mask1)]
    if mask0:
        records = [rec for rec, flag in zip(records, mask0) if flag]
    return records
