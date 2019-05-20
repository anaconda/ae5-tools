# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright 2016 Continuum Analytics, Inc.
#
# All Rights Reserved.
# -----------------------------------------------------------------------------
from __future__ import absolute_import, print_function

import os
import sys

import click
import click_repl
from prompt_toolkit.history import FileHistory

from .commands.project import project
from .commands.revision import revision
from .commands.account import account
from .commands.session import session
from .commands.deployment import deployment
from .commands.job import job
from .commands.run import run
from .commands.user import user

from .login import login_options, get_account
from .format import format_options


@click.group(invoke_without_command=True,
             epilog='Type "ae5 <command> --help" for help on a specific command.')
@login_options()
@format_options()
@click.pass_context
def cli(ctx):
    obj = ctx.ensure_object(dict)
    obj['is_interactive'] = sys.__stdin__.isatty()
    if ctx.invoked_subcommand is None:
        ctx.invoke(repl)


@cli.command(hidden=True)
@login_options()
@click.pass_context
def repl(ctx):
    obj = ctx.ensure_object(dict)
    obj['in_repl'] = True
    click.echo('Anaconda Enterprise 5')
    click.echo('Type "--help" for a list of commands.')
    click.echo('Type "<command> --help" for help on a specific command.')
    hostname, username = get_account(required=False)
    if hostname and username:
        click.echo(f'Active account: {username}@{hostname}')
    click_repl.repl(ctx, prompt_kwargs={'history': FileHistory(os.path.expanduser('~/.ae5/history'))})


cli.add_command(project)
cli.add_command(revision)
cli.add_command(session)
cli.add_command(deployment)
cli.add_command(job)
cli.add_command(run)
cli.add_command(account)
cli.add_command(user)

def main():
    cli(obj={})


if __name__ == '__main__':
    sys.exit(main())
