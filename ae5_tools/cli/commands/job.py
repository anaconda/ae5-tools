import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='list, info, stop',
             epilog='Type "ae5 job <command> --help" for help on a specific command.')
@format_options()
@login_options()
def job():
    pass


@job.command()
@click.argument('job', required=False)
@format_options()
@login_options()
def list(job):
    result = cluster_call('job_list', format='dataframe')
    if job:
        add_param('filter', Identifier.from_string(job).project_filter())
    print_output(result)


@job.command()
@click.argument('job')
@format_options()
@login_options()
def info(job):
    ident = Identifier.from_string(job)
    result = cluster_call('job_info', ident, format='dataframe')
    print_output(result)


@job.command(short_help='Stop and remove a job.')
@click.argument('job')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(job, yes):
    '''Stop a job.'''
    ident = Identifier.from_string(job)
    result = cluster_call('job_info', ident, format='dataframe')
    if not yes:
        yes = click.confirm(f'Stop job {ident}')
    if yes:
        click.echo(f'Stopping {ident}...', nl=False, err=True)
        cluster_call('job_stop', result.id)
        click.echo('stopped.', err=True)
