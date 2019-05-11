import click

from ..login import cluster_call
from ..format import filter_df, print_output, format_options
from ...identifier import Identifier


@click.group()
def revision():
    pass


@revision.command()
@click.argument('project')
@format_options()
def list(project):
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
