import click
import webbrowser

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='list, info, start, stop, open',
             epilog='Type "ae5 session <command> --help" for help on a specific command.')
@format_options()
@login_options()
def session():
    pass


@session.command(short_help='List active sessions.')
@click.argument('session', required=False)
@format_options()
@login_options()
def list(session):
    '''List sessions.

       By default, lists all sessions visible to the authenticated user.
       Simple filters on owner, project name, session id, or project id
       can be performed by supplying an optional SESSION argument. Filters
       on other fields may be applied using the --filter option.
    '''
    result = cluster_call('session_list', format='dataframe')
    if session:
        add_param('filter', Identifier.from_string(session).project_filter(session=True))
    print_output(result)


def single_session(session):
    ident = Identifier.from_string(session)
    return cluster_call('session_info', ident, format='dataframe')


@session.command(short_help='Obtain information about a single session.')
@click.argument('session')
@format_options()
@login_options()
def info(session):
    '''Obtain information about a single session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    result = single_session(session)
    print_output(result)


@session.command(short_help='Start a session for a project.')
@click.argument('project')
@click.option('--no-wait', is_flag=True, help='Do not wait for the session to complete initialization before exiting.')
@click.option('--open', is_flag=True, help='Open the session in a browser upon initialization.')
@click.option('--frameless', is_flag=True, help='Omit the surrounding AE session management frame.')
@format_options()
@login_options()
@click.pass_context
def start(ctx, project, no_wait, open, frameless):
    '''Start a session for a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       By default, this command will wait for the completion of the session
       creation before returning. To return more quickly, use the --no-wait option.
    '''
    if no_wait and open:
        raise click.UsageError('Options --no-wait and --open confict')
    result = cluster_call('project_info', project, format='json')
    response = cluster_call('session_start', result['id'], wait=not no_wait, format='dataframe')
    if open:
        from .session import open as session_open
        ctx.invoke(session_open, session=response['id'], frameless=frameless)
    else:
        print_output(response)


@session.command(short_help='Stop a session.')
@click.argument('session')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(session, yes):
    '''Stop a session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    result = single_session(session)
    if not yes:
        ident = f'{result["owner"]}/{result["name"]}/{result["id"]}'
        yes = click.confirm(f'Stop session {ident}')
    if yes:
        cluster_call('session_stop', result.id)


@session.command(short_help='Open a session in a browser.')
@click.argument('session')
@click.option('--frameless', is_flag=True, help='Omit the surrounding AE session management frame.')
@login_options()
def open(session, frameless):
    '''Opens a session in the default browser.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.

       By default, the session will be opened in a window including the standard
       Anaconda Enterprise project frame. To omit this frame and open only the
       session UI, use the --frameless option.
    '''
    result = single_session(session)
    scheme, _, hostname, _ = result.url.split('/', 3)
    if frameless:
        url = f'{scheme}//{result.session_name}.{hostname}/'
    else:
        _, project_id = result.project_url.rsplit('/', 1)
        url = f'{scheme}//{hostname}/projects/detail/a0-{project_id}/view'
    webbrowser.open(url, 1, True)
