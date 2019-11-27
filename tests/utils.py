import os
import csv
import json
import shlex
import subprocess

from io import StringIO


def _get_vars(*vars):
    missing = [v for v in vars if not os.environ.get(v)]
    if missing:
        raise RuntimeError('The following environment variables must be set: {}'.format(' '.join(missing)))
    result = tuple(os.environ[v] for v in vars)
    return result[0] if len(result) == 1 else result


def _cmd(cmd, table=True):
    # We go through Pandas to CSV to JSON instead of directly to JSON to improve coverage
    cmd = 'coverage run -m ae5_tools.cli.main ' + cmd
    if table:
        cmd += f' --format csv'
    print(f'Executing: {cmd}')
    text = subprocess.check_output(shlex.split(cmd), stdin=open(os.devnull)).decode()
    if not table or not text.strip():
        return text
    result = list(csv.DictReader(StringIO(text)))
    if result and list(result[0].keys()) == ['field', 'value']:
        return {rec['field']: rec['value'] for rec in result}
    return result
