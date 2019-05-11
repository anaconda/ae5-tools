import click

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import filter_df, print_output, format_options
from ...identifier import Identifier


@click.group()
def deployment():
    pass


@deployment.command()
@click.argument('deployment', required=False)
@format_options()
@login_options()
def list(deployment):
    result = cluster_call('deployment_list', format='dataframe')
    if deployment:
        add_param('filter', Identifier.from_string(deployment).project_filter())
    print_output(result)


def single_deployment(deployment):
    ident = Identifier.from_string(deployment)
    return cluster_call('deployment_info', ident, format='dataframe')


@deployment.command()
@click.argument('deployment')
@format_options()
@login_options()
def info(deployment):
    result = single_deployment(deployment)
    print_output(result)


@deployment.command(short_help='Obtain information about a deployment\'s collaborators.')
@click.argument('deployment')
@format_options()
@login_options()
def collaborators(deployment):
    '''Obtain information about a deployment's collaborators. The PROJECT need not be
       fully specified, but it must resolve to a single project.
    '''
    result = cluster_call('deployment_collaborators', deployment, format='dataframe')
    print_output(result)


@deployment.command(short_help='Stop a deployment.')
@click.argument('deployment')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(deployment, yes):
    '''Stop a deployment.'''
    result = single_deployment(deployment)
    ident = f'{result["owner"]}/{result["name"]}/{result["id"]}'
    if not yes:
        yes = click.confirm(f'Stop deployment {ident}')
    if yes:
        click.echo(f'Stopping {ident}...', nl=False)
        cluster_call('deployment_stop', result.id)
        click.echo('done.')



