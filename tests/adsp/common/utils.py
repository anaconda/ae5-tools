from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime

logger = logging.getLogger(__name__)


def _cmd(*cmd, table=True):
    if len(cmd) > 1:
        cmd_str = " ".join(cmd)
    elif isinstance(cmd[0], tuple):
        cmd_str, cmd = " ".join(cmd[0]), cmd[0]
    else:
        cmd_str, cmd = cmd[0], tuple(cmd[0].split())
    print(f"Executing: ae5 {cmd_str}")
    cmd = ("coverage", "run", "--source=ae5_tools", "-m", "ae5_tools.cli.main") + cmd + ("--yes",)
    if table:
        cmd += "--format", "json"
    print(f"Executing: {cmd}")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=open(os.devnull))
    stdoutb, stderrb = p.communicate()
    if p.returncode != 0:
        raise CMDException(cmd_str, p.returncode, stdoutb, stderrb)
    text = stdoutb.decode(encoding="utf-8")
    try:
        return json.loads(text)
    except Exception:
        # Not json parse-able, so return as-is to caller
        pass
    return text

