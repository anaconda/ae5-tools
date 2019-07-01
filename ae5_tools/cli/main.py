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

from .login import login_options, cluster, cluster_call
from .format import format_options, print_output


@click.group(invoke_without_command=True,
             epilog='Type "ae5 <command> --help" for help on a specific command.')
@login_options()
@format_options()
@click.pass_context
def cli(ctx):
    obj = ctx.ensure_object(dict)
    obj['is_interactive'] = sys.__stdin__.isatty()
    obj['is_console'] = sys.__stdout__.isatty()
    if ctx.invoked_subcommand is None:
        ctx.invoke(repl)


@cli.command(hidden=True)
@login_options()
@format_options()
@click.pass_context
def repl(ctx):
    obj = ctx.ensure_object(dict)
    obj['in_repl'] = True
    click.echo('Anaconda Enterprise 5 REPL')
    click.echo('Type "--help" for a list of commands.')
    click.echo('Type "<command> --help" for help on a specific command.')
    click_repl.repl(ctx, prompt_kwargs={'history': FileHistory(os.path.expanduser('~/.ae5/history'))})


@cli.command()
@click.option('--admin', is_flag=True, help='Perform a KeyCloak admin login instead of a user login.')
@login_options()
def login(admin):
    '''Log into the cluster.

       Strictly speaking, this is not necessary, because any other command will
       initiate a login if necessary. Furthermore, if an active session already
       exists for the given hostname/username/password, this will do nothing.
    '''
    cluster(admin=admin)


@cli.command()
@click.option('--admin', is_flag=True, help='Perform a KeyCloak admin login instead of a user login.')
@login_options()
def logout(admin):
    '''Log out of the cluster.

       Sessions automatically time out, but this allows an existing session to
       be closed out to prevent accidental further use.
    '''
    c = cluster(admin=admin, retry=False)
    if c is not None and c.connected:
        c.disconnect()
        click.echo('Logged out.', err=True)


@cli.command()
@click.argument('endpoint')
@login_options()
@format_options()
def call(endpoint):
    '''Make a generic API call. Useful for experimentation. There is no input validation
       nor is there a guarantee that the output will be compatible with the generic
       formatting logic. Currently support GET calls only.
    '''
    result = cluster_call('_api', 'get', endpoint, format='dataframe')
    print_output(result)


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
