import os
import csv
import json
import shlex
import subprocess

from io import StringIO


class CMDException(Exception):
    def __init__(self, cmd, code, stdoutb, stderrb):
        msg = [f'Command returned a non-zero status code {code}']
        msg.append('Command: ' + cmd)
        if stdoutb:
            msg.append('--- STDOUT ---')
            msg.extend(x for x in stdoutb.decode().splitlines())
        if stderrb:
            msg.append('--- STDERR ---')
            msg.extend(x for x in stderrb.decode().splitlines())
        super(CMDException, self).__init__('\n'.join(msg))


def _get_vars(*vars):
    missing = [v for v in vars if not os.environ.get(v)]
    if missing:
        raise RuntimeError('The following environment variables must be set: {}'.format(' '.join(missing)))
    result = tuple(os.environ[v] for v in vars)
    return result[0] if len(result) == 1 else result



def _cmd(cmd, table=True):
    # We go through Pandas to CSV to JSON instead of directly to JSON to improve coverage
    cmd += ' --yes'
    if table:
        cmd += f' --format csv'
    print(f'Executing: ae5 {cmd}')
    cmd = 'coverage run --source=ae5_tools -m ae5_tools.cli.main ' + cmd
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         stdin=open(os.devnull))
    stdoutb, stderrb = p.communicate()
    if p.returncode != 0:
        raise CMDException(cmd, p.returncode, stdoutb, stderrb)
    text = stdoutb.decode()
    if not table or not text.strip():
        return text
    result = list(csv.DictReader(StringIO(text)))
    if result and list(result[0].keys()) == ['field', 'value']:
        return {rec['field']: rec['value'] for rec in result}
    return result
