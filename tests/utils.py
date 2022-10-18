import os
import csv
import json
import shlex
import tarfile
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



def _cmd(*cmd, table=True):
    if len(cmd) > 1:
        cmd_str = ' '.join(cmd)
    elif isinstance(cmd[0], tuple):
        cmd_str, cmd = ' '.join(cmd[0]), cmd[0]
    else:
        cmd_str, cmd = cmd[0], tuple(cmd[0].split())
    print(f'Executing: ae5 {cmd_str}')
    cmd = ('coverage', 'run', '--source=ae5_tools', '-m', 'ae5_tools.cli.main') + cmd + ('--yes',)
    if table:
        cmd += '--format', 'csv'
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         stdin=open(os.devnull))
    stdoutb, stderrb = p.communicate()
    if p.returncode != 0:
        raise CMDException(cmd_str, p.returncode, stdoutb, stderrb)
    text = stdoutb.decode()
    if not table or not text.strip():
        return text
    result = list(csv.DictReader(StringIO(text)))
    if result and list(result[0].keys()) == ['field', 'value']:
        return {rec['field']: rec['value'] for rec in result}
    return result

def _compare_tarfiles(fname1, fname2):
    content = ({}, {})
    for fn, cdict in zip((fname1, fname2), content):
        with tarfile.open(name=fn, mode='r') as tar:
            for tinfo in tar:
                if tinfo.isfile():
                    cdict[tinfo.name.split('/', 1)[1]] = tar.extractfile(tinfo).read()
    if content[0] == content[1]:
        return
    msg = []
    for k in set(content[0]) | set(content[1]):
        c1 = content[0].get(k)
        c2 = content[1].get(k)
        if c1 == c2:
            continue
        if not msg:
            msg.append('Comparing: f1={}, f2={}'.format(fname, fname2))
        if c1 is None or c2 is None:
            msg.append('File {} only found in {}'.format(k, 'f1' if c1 else 'f2'))
        else:
            msg.append('File {} differs: f1: {}B, f2: {}zB'.format(k, len(c1), len(c2)))
    assert False, '\n'.join(msg)
