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
@click.option('--editor', help='The editor to use. If supplied, future sessions will use this editor as well. If not supplied, uses the editor currently selected for the project.')
@click.option('--resource-profile', help='The resource profile to use. If supplied, future sessions will use this resource profile as well. If not supplied, uses the resource profile currently selected for the project.')
@click.option('--wait/--no-wait', default=True, help='Wait for the initialization of the session to complete before returning. For --no-wait, the command will exit as soon as AE acknowledges the session is scheduled.')
@click.option('--open/--no-open', default=False, help='Open a browser upon initialization.')
@click.option('--frame/--no-frame', default=True, help='Include the AE banner when opening.')
@format_options()
@login_options()
@click.pass_context
def start(ctx, project, editor, resource_profile, wait, open, frame):
    '''Start a session for a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       By default, this command will wait for the completion of the session
       creation before returning. To return more quickly, use the --no-wait option.
    '''
    if not wait and open:
        raise click.UsageError('Options --no-wait and --open confict')
    result = cluster_call('project_info', project, format='json')
    patches = {}
    for key, value in (('editor', editor), ('resource_profile', resource_profile)):
        if value and result.get(key) != value:
            patches[key] = value
    if patches:
        cluster_call('project_patch', result['id'], **patches)
    ident = Identifier.from_record(result)
    click.echo(f'Starting session for {ident}...', nl=False, err=True)
    response = cluster_call('session_start', result['id'], wait=wait, format='dataframe')
    if open:
        from .session import open as session_open
        ctx.invoke(session_open, session=response['id'], frame=frame)
    click.echo('started.', err=True)
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
    ident = f'{result["owner"]}/{result["name"]}/{result["id"]}'
    if not yes:
        yes = click.confirm(f'Stop session {ident}', err=True)
    if yes:
        click.echo(f'Stopping {ident}...', nl=False, err=True)
        cluster_call('session_stop', result.id)
        click.echo('stopped.', err=True)


@session.command(short_help='Open an existing session in a browser.')
@click.argument('session')
@click.option('--frame/--no-frame', default=True, help='Include the AE banner.')
@login_options()
def open(session, frame):
    '''Opens a session in the default browser.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.

       By default, the session will be opened in a window including the standard
       Anaconda Enterprise project frame. To omit this frame and open only the
       session UI, use the --frameless option.
    '''
    result = single_session(session)
    scheme, _, hostname, _ = result.url.split('/', 3)
    if frame:
        _, project_id = result.project_url.rsplit('/', 1)
        url = f'{scheme}//{hostname}/projects/detail/a0-{project_id}/view'
    else:
        url = f'{scheme}//{result.session_name}.{hostname}/'
    webbrowser.open(url, 1, True)
