import click
import webbrowser

from ..login import cluster_call
from ..utils import ident_filter, global_options, yes_option


@click.group(short_help='info, list, open, start, stop',
             epilog='Type "ae5 session <command> --help" for help on a specific command.')
@global_options
def session():
    '''Commands related to project development sessions.'''
    pass


@session.command()
@ident_filter('session')
@click.option('--k8s', is_flag=True, help='Include Kubernetes-derived columns (requires additional API calls).')
@global_options
def list(**kwargs):
    '''List active sessions.

       By default, lists all sessions visible to the authenticated user.
       Simple filters on owner, project name, session id, or project id
       can be performed by supplying an optional SESSION argument. Filters
       on other fields may be applied using the --filter option.
    '''
    cluster_call('session_list', **kwargs)


@session.command()
@ident_filter('session', required=True)
@click.option('--k8s', is_flag=True, help='Include Kubernetes-derived columns (requires additional API calls).')
@global_options
def info(**kwargs):
    '''Retreive information about a single session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    cluster_call('session_info', **kwargs)


@session.command()
@ident_filter('session', required=True)
@global_options
def branches(**kwargs):
    '''Retreive information about the git branches for a session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    cluster_call('session_branches', **kwargs)


@session.command()
@ident_filter('session', required=True)
@click.option('--master', is_flag=True, help='Get changes from upstream/master instead of the local session')
@global_options
def changes(**kwargs):
    '''Retreive information about uncommited files in a session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    cluster_call('session_changes', **kwargs)


@session.command()
@ident_filter('project', required=True)
@click.option('--editor', help='The editor to use. If supplied, future sessions will use this editor as well. If not supplied, uses the editor currently selected for the project.')
@click.option('--resource-profile', help='The resource profile to use. If supplied, future sessions will use this resource profile as well. If not supplied, uses the resource profile currently selected for the project.')
@click.option('--wait', is_flag=True, help='Wait/do not wait (default) for the session to complete initialization before exiting.')
@click.option('--open', is_flag=True, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame', is_flag=True, help='Include the AE banner when opening.')
@global_options
def start(**kwargs):
    '''Start a session for a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       By default, this command will wait for the completion of the session
       creation before returning. To return more quickly, use the --no-wait option.
    '''
    response = cluster_call('session_start', **kwargs,
                            prefix='Starting session for {ident}...',
                            postfix='started.')


@session.command()
@ident_filter('session', required=True)
@yes_option
@global_options
def stop(**kwargs):
    '''Stop a session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    cluster_call('session_stop', **kwargs,
                 confirm='Stop session {ident}',
                 prefix='Stopping {ident}...',
                 postfix='stopped.')


@session.command()
@ident_filter('session', required=True)
@click.option('--wait', is_flag=True, help='Wait for the session to complete initialization before exiting.')
@click.option('--open', is_flag=True, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame', is_flag=True, help='Include the AE banner when opening.')
@global_options
def restart(**kwargs):
    '''Restart an existing session.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.
    '''
    result = cluster_call('session_restart', **kwargs,
                          prefix='Restarting session {ident}...',
                          postfix='restarted.')


@session.command()
@ident_filter('session', required=True)
@click.option('--frame', default=True, help='Include the AE banner when opening.')
@global_options
def open(**kwargs):
    '''Open an existing session in a browser.

       The SESSION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.

       By default, the session will be opened in a window including the standard
       Anaconda Enterprise project frame. To omit this frame and open only the
       session UI, use the --frameless option.
    '''
    cluster_call('session_open', **kwargs)
