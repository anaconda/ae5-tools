import atexit
import socket
import subprocess


def register_cleanup(proc):
    def cleanup():
        if proc.returncode is None:
            proc.terminate()
            proc.communicate()

    atexit.register(cleanup)


def raise_error(stdout, stderr, errcode, cmd, msg):
    msg = [msg, "  Command: " + " ".join(cmd), f"  Return code: {errcode}"]
    if stdout:
        msg.append("  ---- STDOUT ---")
        msg.extend("  " + x for x in stdout.splitlines())
    if stderr:
        msg.append("  ---- STDERR ---")
        msg.extend("  " + x for x in stderr.splitlines())
    raise RuntimeError("\n".join(msg))


def launch_background(cmd, waitfor, what, retries=1):
    for attempt in range(retries):
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
        )
        stdout = ""
        for line in proc.stdout:
            stdout += line
            if line.startswith(waitfor):
                register_cleanup(proc)
                return proc
        stderr = proc.stderr.read()
        proc.terminate()
        proc.communicate()
    raise_error(stdout, stderr, proc.returncode, cmd, f"Could not {what}")


def find_remote_port(hostname, username):
    # https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python/2838309#2838309
    cmd = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        f"{username}@{hostname}",
        "python",
        "-c",
        "'" 'import socket;s=socket.socket();s.bind(("", 0));' "print(s.getsockname()[1]);s.close()" "'",
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = proc.communicate()
    errcode = proc.returncode
    if errcode == 0:
        try:
            return int(stdout.splitlines()[0])
        except Exception as exc:
            stderr = stderr.strip()
            stderr += f"\nLocal exception:\n{exc}\n"
    raise_error(stdout, stderr, errcode, cmd, "Could not determine available remote port")


def find_local_port():
    # https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python/2838309#2838309
    s = socket.socket()
    s.bind(("", 0))
    local_port = s.getsockname()[1]
    from .server import K8S_ENDPOINT_PORT

    if local_port == K8S_ENDPOINT_PORT:
        # Don't use 8086 so we can be sure it's available when running this server locally
        local_port = find_local_port()
    s.close()
    return local_port


def tunneled_k8s_url(hostname, username):
    remote_port = find_remote_port(hostname, username)
    local_port = find_local_port()
    cmd = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-t",
        "-t",
        "-L",
        f"{local_port}:localhost:{remote_port}",
        f"{username}@{hostname}",
        "kubectl",
        "proxy",
        "--disable-filter",
        f"--port={remote_port}",
    ]
    proc = launch_background(cmd, "Starting to serve", "establish k8s proxy", retries=3)
    return proc, f"http://localhost:{local_port}"
