import click
import webbrowser

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import filter_df, print_output, format_options
from ...identifier import Identifier


@click.group()
def session():
    pass


@session.command()
@click.argument('session', required=False)
@format_options()
@login_options()
def list(session):
    result = cluster_call('session_list', format='dataframe')
    if session:
        add_param('filter', Identifier.from_string(session).project_filter())
    print_output(result)


def single_session(session):
    ident = Identifier.from_string(session)
    return cluster_call('session_info', ident, format='dataframe')


@session.command()
@click.argument('session')
@format_options()
@login_options()
def info(session):
    result = single_session(session)
    print_output(result)


@session.command(short_help='Start a session for a project.')
@click.argument('project')
@login_options()
def start(project):
    '''Start a session for a project.'''
    result = cluster_call('project_info', project, format='json')
    ident = f'{result["owner"]}/{result["name"]}/{result["id"]}'
    click.echo(f'Starting a session for {ident}...', nl=False)
    cluster_call('session_start', result['id'], wait=True)
    click.echo(f'done.')


@session.command(short_help='Stop a session.')
@click.argument('session')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(session, yes):
    '''Stop a session.'''
    result = single_session(session)
    ident = f'{result["owner"]}/{result["name"]}/{result["id"]}'
    if not yes:
        yes = click.confirm(f'Stop session {ident}')
    if yes:
        click.echo(f'Stopping {ident}...', nl=False)
        cluster_call('session_stop', result.id)
        click.echo('done.')


@session.command()
@click.argument('session')
@click.option('--frameless', is_flag=True, default=False, help='Omit the surrounding AE session management frame.')
@format_options()
@login_options()
def open(session, frameless):
    result = single_session(session)
    scheme, _, hostname, _ = result.url.split('/', 3)
    if frameless:
        url = f'{scheme}//{result.session_name}.{hostname}/'
    else:
        _, project_id = result.project_url.rsplit('/', 1)
        url = f'{scheme}//{hostname}/projects/detail/a0-{project_id}/view'
    webbrowser.open(url, 1, True)

