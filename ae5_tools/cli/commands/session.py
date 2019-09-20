import click
import webbrowser

from ..login import cluster_call, login_options
from ..utils import ident_filter
from ..format import format_options


@click.group(short_help='info, list, open, start, stop',
             epilog='Type "ae5 session <command> --help" for help on a specific command.')
@format_options()
@login_options()
def session():
    '''Commands related to project development sessions.'''
    pass


@session.command(short_help='List active sessions.')
@ident_filter('session')
@format_options()
@login_options()
def list():
    '''List sessions.

       By default, lists all sessions visible to the authenticated user.
       Simple filters on owner, project name, session id, or project id
       can be performed by supplying an optional SESSION argument. Filters
       on other fields may be applied using the --filter option.
    '''
    cluster_call('session_list', cli=True)


@session.command(short_help='Obtain information about a single session.')
@click.argument('session')
@format_options()
@login_options()
def info(session):
    '''Retreive information about a single session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    cluster_call('session_info', session, cli=True)


def _open(record, frame):
    if isinstance(record, tuple):
        record = {k: v for k, v in record[0]}
    if frame:
        scheme, _, hostname, *_, project_id = record['project_url'].split('/')
        url = f'{scheme}//{hostname}/projects/detail/a0-{project_id}/view'
    else:
        scheme, _, hostname, *_, session_id = record['url'].split('/')
        url = f'{scheme}//{session_id}.{hostname}/'
    webbrowser.open(url, 1, True)


@session.command(short_help='Start a session for a project.')
@click.argument('project')
@click.option('--editor', help='The editor to use. If supplied, future sessions will use this editor as well. If not supplied, uses the editor currently selected for the project.')
@click.option('--resource-profile', help='The resource profile to use. If supplied, future sessions will use this resource profile as well. If not supplied, uses the resource profile currently selected for the project.')
@click.option('--wait', is_flag=True, help='Wait for the session to complete initialization before exiting.')
@click.option('--open', is_flag=True, help='Open a browser upon initialization. Implies --wait.')
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
    response = cluster_call('session_start', ident=project, id_class='project', wait=wait,
                            editor=editor, resource_profile=resource_profile,
                            prefix='Starting session for {ident}...',
                            postfix='started.', cli=True)
    if open:
        _open(response, frame)


@session.command(short_help='Stop a session.')
@click.argument('session')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@format_options()
@login_options()
def stop(session, yes):
    '''Stop a session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    cluster_call('session_stop', ident=session,
                 confirm='Stop session {ident}',
                 prefix='Stopping {ident}...',
                 postfix='stopped.', cli=True)


@session.command(short_help='Restart a session for a project.')
@click.argument('session')
@click.option('--wait', is_flag=True, help='Wait for the session to complete initialization before exiting.')
@click.option('--open', is_flag=True, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame/--no-frame', default=True, help='Include the AE banner when opening.')
@format_options()
@login_options()
@click.pass_context
def restart(ctx, session, wait, open, frame):
    '''Restart a deployment for a project.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('session_restart', ident=session,
                          wait=wait or open,
                          prefix='Restarting session {ident}...',
                          postfix='restarted.')
    if open:
        _open(result, frame)


@session.command(short_help='Open an existing session in a browser.')
@click.argument('session')
@click.option('--frame/--no-frame', default=True, help='Include the AE banner when opening.')
@format_options()
@login_options()
def open(session, frame):
    '''Opens a session in the default browser.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.

       By default, the session will be opened in a window including the standard
       Anaconda Enterprise project frame. To omit this frame and open only the
       session UI, use the --frameless option.
    '''
    result = cluster_call('session_info', session, format='json')
    _open(result, frame)
