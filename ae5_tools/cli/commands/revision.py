import click

from ..login import cluster_call, login_options
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='list, info, download, image',
             epilog='Type "ae5 revision <command> --help" for help on a specific command.')
@format_options()
@login_options()
def revision():
    pass


@revision.command()
@click.argument('project')
@format_options()
def list(project):
    '''List available revisions for a given project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('revision_list', project, format='dataframe')
    print_output(result)


@revision.command()
@click.argument('revision')
@format_options()
def info(revision):
    result = cluster_call('revision_info', revision, format='dataframe')
    print_output(result)


@revision.command()
@click.argument('revision')
@click.option('--filename', default='', help='Filename')
def download(revision, filename):
    ident = Identifier.from_string(revision)
    record = cluster_call('revision_info', ident, format='json')
    _, pid, _, rev = record['url'].rsplit('/', 3)
    pid = 'a0-' + pid
    if not filename:
        revdash = f'-{rev}' if ident.revision not in ('', 'latest', '*') else ''
        project = cluster_call('project_info', pid, format='json')
        name = project['name'] if ident.name or not ident.id else project['id']
        filename = f'{name}{revdash}.tar.gz'
    cluster_call('project_download', f'{pid}:{rev}', filename=filename)
    print(f'File {filename} downloaded.')


@revision.command()
@click.argument('revision')
@click.option('--command', default='', help='Command name to execute.')
@click.option('--use_anaconda_cloud', is_flag=True, help='public repo')
@click.option('--dockerfile', default='', help='dockerfile path')
@click.option('--debug', is_flag=True, help='debug logs')
def image(revision, command, use_anaconda_cloud, dockerfile, debug):
    ident = Identifier.from_string(revision)
    record = cluster_call('revision_info', ident, format='json')
    _, pid, _, rev = record['url'].rsplit('/', 3)
    pid = 'a0-' + pid

    cluster_call('project_image', f'{pid}:{rev}', command=command, use_anaconda_cloud=use_anaconda_cloud, dockerfile_path=dockerfile, debug=debug)

