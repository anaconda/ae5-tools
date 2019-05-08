import click

from .utils import cluster, filter_df, sort_df, print_output, format_options, Identifier, add_param


@click.group()
def deployment():
    pass


@deployment.command()
@click.argument('deployment', required=False)
@format_options()
def list(deployment):
    result = cluster().deployments(format='dataframe')
    if deployment:
        add_param('filter', Identifier.from_string(deployment).project_filter())
    print_output(result)


def single_deployment(deployment):
    ident = Identifier.from_string(deployment)
    result = cluster().deployments(format='dataframe')
    result = filter_df(result, ident.project_filter())
    if len(result) == 0:
        raise click.UsageError(f'Deployment not found: {deployment}')
    elif len(result) > 1:
        raise click.UsageError(f'Multiple deployments found matcing {deployment}; specify owner?')
    return result.astype(object).iloc[0]


@deployment.command()
@click.argument('deployment')
@format_options()
def info(deployment):
    result = single_deployment(deployment)
    print_output(result)

