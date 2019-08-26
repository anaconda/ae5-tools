import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='info, list, log, stop, delete',
             epilog='Type "ae5 run <command> --help" for help on a specific command.')
@format_options()
@login_options()
def run():
    '''list, info, stop'''
    pass


@run.command(short_help='List the available runs.')
@click.argument('run', required=False)
@format_options()
@login_options()
def list(run):
    if run:
        ident = Identifier.from_string(run, no_revision=True)
        add_param('filter', ident.project_filter())
    cluster_call('run_list', cli=True)


@run.command(short_help='Obtain information about a single run.')
@click.argument('run')
@format_options()
@login_options()
def info(run):
    cluster_call('run_info', run, cli=True)


@run.command(short_help='Retrieve the log for a single run.')
@click.argument('run')
@format_options()
@login_options()
def log(run):
    cluster_call('run_log', run, cli=True)


@run.command(short_help='Stop a run.')
@click.argument('run')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(run, yes):
    '''Stop a run.'''
    cluster_call('run_stop', ident=run,
                 confirm=None if yes else 'Stop run {ident}',
                 prefix='Stopping run {ident}...',
                 postfix='stopped.', cli=True)


@run.command(short_help='Delete the record of a run.')
@click.argument('run')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def delete(run, yes):
    '''Stop a run.'''
    cluster_call('run_delete', ident=run,
                 confirm=None if yes else 'Delete run {ident}',
                 prefix='Deleting run {ident}...',
                 postfix='deleted.', cli=True)

