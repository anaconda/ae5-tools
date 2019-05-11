import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import filter_df, print_output, format_options
from ...identifier import Identifier


@click.group()
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


def single_job(job):
    ident = Identifier.from_string(job)
    return cluster_call('job_info', job, format='dataframe')


@job.command()
@click.argument('job')
@format_options()
@login_options()
def info(job):
    result = single_job(job)
    print_output(result)


@job.command(short_help='Stop and remove a job.')
@click.argument('job')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(job, yes):
    '''Stop a job.'''
    result = single_job(job)
    if not yes:
        yes = click.confirm(f'Stop job {ident}')
    if yes:
        click.echo(f'Stopping {ident}...', nl=False)
        cluster_call('job_stop', result.id)
        click.echo('done.')
