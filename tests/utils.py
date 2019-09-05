import os
import json
import shlex
import subprocess
import pandas as pd

from io import BytesIO


def _get_vars(*vars):
    missing = [v for v in vars if not os.environ.get(v)]
    if missing:
        raise RuntimeError('The following environment variables must be set: {}'.format(' '.join(missing)))
    result = tuple(os.environ[v] for v in vars)
    return result[0] if len(result) == 1 else result


def _cmd(cmd, table=True):
    # We go through Pandas to CSV to JSON instead of directly to JSON to improve coverage
    cmd = 'ae5 ' + cmd
    if table:
        cmd += f' --format csv'
    print(f'Executing: {cmd}')
    text = subprocess.check_output(shlex.split(cmd), stdin=open(os.devnull))
    if not table or not text.strip():
        return text.decode()
    csv = pd.read_csv(BytesIO(text)).fillna('').astype(str)
    if tuple(csv.columns) == ('field', 'value'):
        return csv.set_index('field').T.iloc[0].to_dict()
    return json.loads(csv.to_json(index=False, orient='table'))['data']
