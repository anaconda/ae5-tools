import click

from ..login import cluster_call
from ..utils import add_param, global_options, ident_filter
from ...identifier import Identifier


@click.group(short_help='delete, info, list, log, stop',
             epilog='Type "ae5 run <command> --help" for help on a specific command.')
@global_options
def run():
    '''Commands related to run records.'''
    pass


@run.command()
@ident_filter('run')
@global_options
def list():
    '''List all available run records.

       By default, lists all runs visible to the authenticated user.
       Simple filters on owner, run name, or id can be performed by
       supplying an optional RUN argument. Filters on other fields may
       be applied using the --filter option.
    '''
    cluster_call('run_list', cli=True)


@run.command()
@click.argument('run')
@global_options
def info(run):
    '''Retrieve information about a single run.

       The RUN identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one run.
    '''
    cluster_call('run_info', run, cli=True)


@run.command(short_help='Retrieve the log for a single run.')
@click.argument('run')
@global_options
def log(run):
    '''Retrieve the log file for a particular run.

       The RUN identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one run.
    '''
    cluster_call('run_log', run, cli=True)


@run.command()
@click.argument('run')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@global_options
def stop(run, yes):
    '''Stop a run.

       Does not produce an error if the run has already completed.

       The RUN identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one run.
    '''
    cluster_call('run_stop', ident=run,
                 confirm=None if yes else 'Stop run {ident}',
                 prefix='Stopping run {ident}...',
                 postfix='stopped.', cli=True)


@run.command()
@click.argument('run')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@global_options
def delete(run, yes):
    '''Delete a run record.

       The RUN identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one run.
    '''
    cluster_call('run_delete', ident=run,
                 confirm=None if yes else 'Delete run {ident}',
                 prefix='Deleting run {ident}...',
                 postfix='deleted.', cli=True)
