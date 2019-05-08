import click

from .utils import cluster, filter_df, sort_df, print_output, format_options, Identifier, add_param


@click.group()
def job():
    pass


@job.command()
@click.argument('job', required=False)
@format_options()
def list(job):
    result = cluster().jobs(format='dataframe')
    if job:
        add_param('filter', Identifier.from_string(job).project_filter())
    print_output(result)


def single_job(job):
    ident = Identifier.from_string(job)
    result = cluster().jobs(format='dataframe')
    result = filter_df(result, ident.project_filter())
    if len(result) == 0:
        raise click.UsageError(f'Job not found: {job}')
    elif len(result) > 1:
        raise click.UsageError(f'Multiple jobs found matcing {job}; specify owner?')
    return result.astype(object).iloc[0]


@job.command()
@click.argument('job')
@format_options()
def info(job):
    result = single_job(job)
    print_output(result)

