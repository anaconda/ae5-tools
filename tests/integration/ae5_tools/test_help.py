import os
import shlex
import subprocess


def _cmd(cmd, table=True):
    # We go through Pandas to CSV to JSON instead of directly to JSON to improve coverage
    cmd = "ae5 " + cmd
    print(f"Executing: {cmd}")
    text = subprocess.check_output(shlex.split(cmd), stdin=open(os.devnull))
    assert text.strip() != ""
    print(text.decode())


def test_help():
    _cmd("--help")


def test_help_login():
    _cmd("--help-login")


def test_help_format():
    _cmd("--help-format")


def test_project_help():
    _cmd("project --help")


def test_session_help():
    _cmd("session --help")


def test_deployment_help():
    _cmd("deployment --help")
