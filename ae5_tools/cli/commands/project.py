import click

from ..utils import add_param
from ..login import login_options, cluster_call
from ..format import filter_df, print_output, format_options
from ...identifier import Identifier


@click.group(short_help='List, examine, download, upload, and delete projects.',
             epilog='Type "ae5 project <command> --help" for help on a specific command.')
@login_options()
def project():
    '''List, examine, download, upload, and delete projects.'''
    pass


@project.command(short_help='List all available projects.')
@click.argument('project', required=False)
@format_options()
@login_options()
def list(project):
    '''List all available projects. Simple filters on owner, project name,
       or id can be performed by supplying an optional PROJECT argument.
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
    '''Obtain information about a single project. The PROJECT need not be fully
       specified, but it must resolve to a single project.
    '''
    result = cluster_call('project_info', project, format='dataframe')
    print_output(result)


@project.command(short_help='Retrieve the project collaborators.')
@click.argument('project')
@format_options()
@login_options()
def collaborators(project):
    '''Obtain information about a project's collaborators. The PROJECT need not be
       fully specified, but it must resolve to a single project.
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
    '''Retrieve the project's acitivty log. The PROJECT need not be
       fully specified, but it must resolve to a single project.
    '''
    result = cluster_call('project_activity', project, limit=0 if all else limit, format='dataframe')
    print_output(result)


@project.command(short_help='Retrieve the latest activity entry.')
@click.argument('project')
@format_options()
@login_options()
def status(project):
    '''Retrieve the project's latest activity entry. The PROJECT need not be
       fully specified, but it must resolve to a single project.
    '''
    result = cluster_call('project_activity', project, latest=True, format='dataframe')
    print_output(result)


@project.command(short_help='Start a session for a project.')
@click.argument('project')
@click.option('--wait/--no-wait', default=True, help='Wait for the session to complete initialization before exiting.')
@format_options()
@login_options()
def start(project, wait):
    '''Start a session for a project.'''
    from .session import start as session_start
    ctx.invoke(session_start, wait=wait)


@project.command(short_help='Download an archive of a project.')
@click.argument('project')
@click.option('--filename', default='', help='Filename to save to. If not supplied, the filename is constructed from the name of the project.')
@login_options()
@click.pass_context
def download(ctx, project, filename):
    '''Download a project. By default, the latest revision of PROJECT
       will be downloaded, but a revision value may optionally be specified.
    '''
    from .revision import download as revision_download
    ctx.invoke(revision_download, revision=project, filename=filename)


@project.command(short_help='Upload a project to the cluster.')
@click.argument('filename', type=click.Path(exists=True))
@click.option('--name', default='', help='Name of the project.')
@login_options()
def upload(filename, name):
    '''Upload a project. A new project entry is created, so the name of the
       project must be unique. The name of the project is taken from the basename
       of the file, but can be overridden with the --name option.
    '''
    print(f'Uploading {filename}...')
    cluster_call('project_upload', filename, name=name or None, wait=True)
    print('done.')


@project.command(short_help='Delete a project.')
@click.argument('project')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def delete(project, yes):
    '''Delete a project.'''
    result = cluster_call('project_info', project, format='json')
    ident = Identifier.from_record(result)
    if not yes:
        yes = click.confirm(f'Delete project {ident}')
    if yes:
        click.echo(f'Deleting {ident}...', nl=False)
        result = cluster_call('project_delete', result['id'])
        click.echo('done.')
