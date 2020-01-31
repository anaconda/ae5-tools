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
from .commands.account import account
from .commands.session import session
from .commands.sample import sample
from .commands.endpoint import endpoint
from .commands.editor import editor
from .commands.deployment import deployment
from .commands.resource_profile import resource_profile
from .commands.job import job
from .commands.run import run
from .commands.user import user
from .commands.node import node
from .commands.pod import pod

from .login import login_options, cluster_call, cluster_disconnect
from .format import format_options
from .utils import stash_defaults, global_options
from .._version import get_versions


version = get_versions().get('version', 'UNKNOWN')
# todo: Add prog_name and start using these everywhere
SHORT_BRAND = "AE5"
LONG_BRAND = "Anaconda Enterprise 5"


@click.group(invoke_without_command=True,
             epilog='Type "ae5 <command> --help" for help on a specific command.')
@click.version_option(version=version, message="%(prog)s %(version)s")
@global_options
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        ctx.invoke(repl)


@cli.command(hidden=True)
@global_options
@click.pass_context
def repl(ctx):
    stash_defaults()
    click.echo(f'{LONG_BRAND} REPL')
    click.echo('Type "--help" for a list of commands.')
    click.echo('Type "<command> --help" for help on a specific command.')
    click_repl.repl(ctx, prompt_kwargs={'history': FileHistory(os.path.expanduser('~/.ae5/history'))})


@cli.command()
@click.option('--admin', is_flag=True, help='Perform a KeyCloak admin login instead of a user login.')
@global_options
def login(admin):
    '''Log into the cluster.

       Strictly speaking, this is not necessary, because any other command will
       initiate a login if necessary. Furthermore, if an active session already
       exists for the given hostname/username/password, this will do nothing.
    '''
    # Execute a simple API call to force authentication
    call = 'user_list' if admin else 'run_list'
    cluster_call(call, admin=admin)


@cli.command()
@click.option('--admin', is_flag=True, help='Perform a KeyCloak admin login instead of a user login.')
@global_options
def logout(admin):
    '''Log out of the cluster.

       Sessions automatically time out, but this allows an existing session to
       be closed out to prevent accidental further use.
    '''
    cluster_disconnect(admin)


@cli.command()
@click.argument('path')
@click.option('--endpoint', help='An endpoint to connect to instead of the default API.')
@click.option('--post', is_flag=True, help='Do a POST instead of a GET.')
@global_options
def call(path, endpoint, post):
    '''Make a generic API call. This is useful for experimentation with the
       AE5 API. However, it is particularly useful for accessing REST APIs
       delivered as private deployments, because it handles authentication.

       The PATH argument looks like a standard URL path, without hostname or
       scheme. However, if the path does not begin with a slash '/', this
       component is assumed to be the subdomain for the deployment.

       For instance, if the hostname is anaconda.test.com,
          /api/v2/runs -> https://anaconda.test.com/api/v2/runs
          deployment1  -> https://deployment1.anaconda.test.com/
          deployment2/test/me -> https://deployment2.anaconda.test.com/test/me

       There is no input validation, nor is there a guarantee that the output will
       be compatible with the generic formatting logic. Only GET calls are currently
       supported.
    '''
    if endpoint and not path.startswith('/'):
        path = '/' + path
    method = 'post' if post else 'get'
    cluster_call('api', method, path, subdomain=endpoint)


cli.add_command(project)
cli.add_command(sample)
cli.add_command(endpoint)
cli.add_command(session)
cli.add_command(deployment)
cli.add_command(job)
cli.add_command(run)
cli.add_command(account)
cli.add_command(user)
cli.add_command(resource_profile)
cli.add_command(editor)
cli.add_command(node)
cli.add_command(pod)


def main():
    cli(obj={})


if __name__ == '__main__':
    sys.exit(main())
