import click

from ..utils import add_param
from ..login import login_options, cluster_call
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='List, examine, download, upload, and delete projects.',
             epilog='Type "ae5 project <command> --help" for help on a specific command.')
def project():
    '''List, examine, download, upload, and delete projects.'''
    pass


@project.command(short_help='List available projects.')
@click.argument('project', required=False)
@format_options()
@login_options()
def list(project):
    '''List available projects.

       By default, lists all projects visible to the authenticated user.
       Simple filters on owner, project name, or id can be performed by
       supplying an optional PROJECT argument. Filters on other fields may
       be applied using the --filter option.
    '''
    result = cluster_call('project_list', format='dataframe')
    if project:
        add_param('filter', Identifier.from_string(project).project_filter())
    print_output(result)


@project.command(short_help='Obtain information about a single project.')
@click.argument('project')
@format_options()
@login_options()
def info(project):
    '''Obtain information about a single project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_info', project, format='dataframe')
    print_output(result)


@project.command(short_help='Retrieve the project collaborators.')
@click.argument('project')
@format_options()
@login_options()
def collaborators(project):
    '''Obtain information about a project's collaborators.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_collaborators', project, format='dataframe')
    print_output(result)


@project.command(short_help='Retrieve the activity log.')
@click.argument('project')
@click.option('--limit', type=int, default=10, help='Limit the output to N records.')
@click.option('--all', is_flag=True, default=False, help='Retrieve all possible records.')
@format_options()
@login_options()
def activity(project, limit, all):
    '''Retrieve the project's acitivty log.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       By default, the latest 10 records will be returned. This behavior can be
       adjusted using the --limit or --all options.
    '''
    result = cluster_call('project_activity', project, limit=0 if all else limit, format='dataframe')
    print_output(result)


@project.command(short_help='Retrieve the latest activity entry.')
@click.argument('project')
@format_options()
@login_options()
def status(project):
    '''Retrieve the project's latest activity entry.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_activity', project, latest=True, format='dataframe')
    print_output(result)


@project.command(short_help='Start a session for a project.')
@click.argument('project')
@click.option('--wait/--no-wait', default=True, help='Wait for the session to complete initialization before exiting.')
@format_options()
@login_options()
@click.pass_context
def start(ctx, project, wait):
    '''Start a session for a project.'''
    from .session import start as session_start
    ctx.invoke(session_start, project, wait=wait)


@project.command(short_help='Download an archive of a project.')
@click.argument('project')
@click.option('--filename', default='', help='Filename to save to. If not supplied, the filename is constructed from the name of the project.')
@login_options()
@click.pass_context
def download(ctx, project, filename):
    '''Download a project given by the identifier PROJECT.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       A revision value may optionally be supplied in the PROJECT identifier.
       If not supplied, the latest revision will be selected.
    '''
    from .revision import download as revision_download
    ctx.invoke(revision_download, revision=project, filename=filename)


@project.command(short_help='Upload a project to the cluster.')
@click.argument('filename', type=click.Path(exists=True))
@click.option('--name', default='', help='Name of the project.')
@format_options()
@login_options()
def upload(filename, name):
    '''Upload a project.

       By default, the name of the project is taken from the basename of
       the file. This can be overridden by using the --name option. The
       name must not be the same as an existing project.
    '''
    print(f'Uploading {filename}...')
    result = cluster_call('project_upload', filename, name=name or None, wait=True, format='dataframe')
    print_output(result)


@project.command(short_help='Delete a project.')
@click.argument('project')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def delete(project, yes):
    '''Delete a project. The supplied PROJECT identifier may include wildcards,
       but it must resolve to a single project.
    '''
    result = cluster_call('project_info', project, format='json')
    ident = Identifier.from_record(result)
    if not yes:
        yes = click.confirm(f'Delete project {ident}')
    if yes:
        click.echo(f'Deleting {ident}...')
        result = cluster_call('project_delete', result['id'])
        click.echo('done.')
