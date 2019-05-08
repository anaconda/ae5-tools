# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright 2016 Continuum Analytics, Inc.
#
# All Rights Reserved.
# -----------------------------------------------------------------------------
from __future__ import absolute_import, print_function

import os
import sys
import time

import click
import click_repl
from prompt_toolkit.history import FileHistory

from .project import project
from .revision import revision
from .credentials import credentials
from .session import session
from .deployment import deployment
from .job import job
from .utils import cluster, login_options


@click.group(invoke_without_command=True)
@login_options()
@click.pass_context
def cli(ctx):
    obj = ctx.ensure_object(dict)
    obj['is_interactive'] = sys.__stdin__.isatty()
    if ctx.invoked_subcommand is None:
        ctx.invoke(repl)


@cli.command()
@click.pass_context
def repl(ctx):
    obj = ctx.ensure_object(dict)
    obj['in_repl'] = True
    click.echo('Anaconda Enterprise 5')
    click.echo('Type "--help" for a list of commands.')
    click.echo('Type "<command> --help" for help on a specific command.')
    prompt_kwargs = {
        'history': FileHistory(os.path.expanduser('~/.ae5/history'))
    }
    click_repl.repl(click.get_current_context(), prompt_kwargs=prompt_kwargs)


@cli.command()
def abort():
    cluster()
    print('Sleeping')
    time.sleep(5)
    sys.exit(1)


cli.add_command(project)
cli.add_command(revision)
cli.add_command(credentials)
cli.add_command(session)
cli.add_command(deployment)
cli.add_command(job)


def main():
    cli(obj={})


if __name__ == '__main__':
    sys.exit(main())
