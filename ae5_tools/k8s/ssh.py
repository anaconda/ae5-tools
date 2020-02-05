import subprocess
import socket
import atexit
import time
import os
import sys


def register_cleanup(proc):
    def cleanup():
        proc.terminate()
        proc.communicate()
    atexit.register(cleanup)


def raise_error(stdout, stderr, errcode, cmd, msg):
    msg = [msg, '  Command: ' + ' '.join(cmd),
           f'  Return code: {errcode}']
    if stdout:
        msg.append('  ---- STDOUT ---')
        msg.extend('  ' + x for x in stdout.splitlines())
    if stderr:
        msg.append('  ---- STDERR ---')
        msg.extend('  ' + x for x in stderr.splitlines())
    raise RuntimeError('\n'.join(msg))


def find_remote_port(hostname, username):
    # https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python/2838309#2838309
    cmd = ['ssh', '-o', 'StrictHostKeyChecking=no',
           f'{username}@{hostname}', 'python', '-c',
           "'" 'import socket;s=socket.socket();s.bind(("", 0));'
           'print(s.getsockname()[1]);s.close()'"'"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True)
    stdout, stderr = proc.communicate()
    errcode = proc.returncode
    if errcode == 0:
        try:
            return int(stdout.splitlines()[0])
        except Exception as exc:
            stderr = stderr.strip()
            stderr += f'\nLocal exception:\n{exc}\n'
    raise_error(stdout, stderr, errcode, cmd,
                'Could not determine available remote port')


def find_local_port():
    # https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python/2838309#2838309
    s = socket.socket()
    s.bind(("", 0))
    local_port = s.getsockname()[1]
    s.close()
    return local_port


def tunneled_k8s_url(hostname, username):
    """Race condition on finding a port and binging to it, so retry as a hack"""
    retry = 3
    captured_exception = None
    for i in range(retry):
        try:
            return _tunneled_k8s_url(hostname, username)
        except RuntimeError as e:
            captured_exception = e
    raise captured_exception


def _tunneled_k8s_url(hostname, username):
    remote_port = find_remote_port(hostname, username)
    local_port = find_local_port()
    cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-t', '-t', '-L',
           f'{local_port}:localhost:{remote_port}', f'{username}@{hostname}',
           'kubectl', 'proxy', '--disable-filter', f'--port={remote_port}']
    proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True)
    stdout = ''
    for line in proc.stdout:
        stdout += line
        if line.startswith('Starting to serve'):
            register_cleanup(proc)
            return f'http://localhost:{local_port}'
    raise_error(stdout, proc.stderr.read(), proc.returncode, cmd,
                'Could not establish k8s proxy')
