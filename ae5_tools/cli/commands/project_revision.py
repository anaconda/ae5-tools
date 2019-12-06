import click

from ..login import cluster_call
from ..utils import global_options
from ...identifier import Identifier


@click.group(short_help='Subcommands: download, image, info, list',
             epilog='Type "ae5 project revision <command> --help" for help on a specific command.')
@global_options
def revision():
    '''Commands related to the revisions of a project.'''
    pass


@revision.command()
@click.argument('project')
@global_options
def list(project):
    '''List available revisions for a given project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('revision_list', project, cli=True)


@revision.command()
@click.argument('revision')
@global_options
def info(revision):
    '''Retrieve information about a single project revision.

       The REVISION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project. If the revision is not
       specified, then the latest will be assumed.
    '''
    cluster_call('revision_info', revision, cli=True)


@revision.command()
@click.argument('revision')
@global_options
def commands(revision):
    '''List the commands for a given project revision.

       The REVISION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project. If the revision is not
       specified, then the latest will be assumed.
    '''
    cluster_call('revision_commands', revision, cli=True)


@revision.command()
@click.argument('revision')
@click.option('--filename', default='', help='Filename')
@global_options
def download(revision, filename):
    '''Download a project revision.

       The REVISION identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project. If the revision is not
       specified, the latest will be assumed.
    '''
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
@click.option('--condarc', default='', help='Path to custom condarc file.')
@click.option('--dockerfile', default='', help='Path to custom Dockerfile.')
@click.option('--debug', is_flag=True, help='debug logs')
@global_options
def image(revision, command, condarc, dockerfile, debug):
    ident = Identifier.from_string(revision)
    record = cluster_call('revision_info', ident, format='json')
    _, pid, _, rev = record['url'].rsplit('/', 3)
    pid = 'a0-' + pid
    cluster_call('project_image', f'{pid}:{rev}', command=command,
                 condarc_path=condarc, dockerfile_path=dockerfile,
                 debug=debug, cli=True)

