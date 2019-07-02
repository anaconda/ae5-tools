import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='list, info, stop',
             epilog='Type "ae5 run <command> --help" for help on a specific command.')
@format_options()
@login_options()
def run():
    '''list, info, stop'''
    pass


@run.command()
@click.argument('run', required=False)
@format_options()
@login_options()
def list(run):
    result = cluster_call('run_list', format='dataframe')
    if run:
        add_param('filter', Identifier.from_string(run).project_filter())
    print_output(result)


@run.command()
@click.argument('run')
@format_options()
@login_options()
def info(run):
    ident = Identifier.from_string(run)
    result = cluster_call('run_info', ident, format='dataframe')
    print_output(result)


@run.command(short_help='Stop and remove a run.')
@click.argument('run')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(run, yes):
    '''Stop a run.'''
    ident = Identifier.from_string(run)
    result = cluster_call('run_info', ident, format='dataframe')
    if not yes:
        yes = click.confirm(f'Stop run {ident}')
    if yes:
        click.echo(f'Stopping {ident}...', nl=False, err=True)
        cluster_call('run_stop', result.id)
        click.echo('stopped.', err=True)
