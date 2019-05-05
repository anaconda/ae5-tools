# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright 2016 Continuum Analytics, Inc.
#
# All Rights Reserved.
# -----------------------------------------------------------------------------
from __future__ import absolute_import, print_function

import os
import sys
import shutil
import logging
import pandas as pd

import click
import click_repl
from prompt_toolkit.history import FileHistory

from ..config import config
from ..api import AECluster
from .utils import print_df


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    if 'cluster' not in ctx.obj:
        hostname, username, password = config.find()
        ctx.obj['cluster'] = AECluster(hostname, username, password)
    if ctx.invoked_subcommand is None:
        ctx.invoke(repl)

@cli.command()
def repl():
    prompt_kwargs = {
        'history': FileHistory(os.path.expanduser('~/.ae5/history'))
    }
    click_repl.repl(click.get_current_context(), prompt_kwargs=prompt_kwargs)


from .project import project
from .revision import revision
cli.add_command(project)
cli.add_command(revision)


def main():
    cli(obj={})


if __name__ == '__main__':
    sys.exit(main())
