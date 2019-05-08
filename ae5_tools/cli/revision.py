import click

from .utils import cluster, filter_df, print_output, format_options, Identifier
from .project import single_project


@click.group()
def revision():
    pass


@revision.command()
@click.argument('project')
@format_options()
def list(project):
    id = single_project(project).id
    result = cluster().project_revisions(id=id, format='dataframe')
    print_output(result)


def single_revision(revision, strict=True):
    ident = revision if isinstance(revision, Identifier) else Identifier.from_string(revision)
    project = single_project(ident.to_string(drop_revision=True))
    result = cluster().project_revisions(id=project['id'], format='dataframe')
    if ident.revision == 'latest' or not ident.revision and not strict:
        result = result.iloc[[0]]
    else:
        result = filter_df(result, ident.revision_filter())
    if len(result) == 0:
        raise click.UsageError(f'Revision not found: {revision}')
    elif len(result) > 1:
        raise click.UsageError(f'Multiple revisions found matcing {revision}')
    return result.iloc[0], project


@revision.command()
@click.argument('revision')
@format_options()
def info(revision):
    revision, project = single_revision(revision, strict=False)
    print_output(revision)


@revision.command()
@click.argument('revision')
@click.option('--filename', default='', help='Filename')
def download(revision, filename):
    ident = Identifier.from_string(revision)
    if not ident.revision:
        raise click.UsageError('Explicit revision value is required')
    revision, project = single_revision(revision)
    if not filename:
        revdash = '-{ident.revision}' if ident.revision and ident.revision != 'latest' else ''
        filename = f'{ident.name}{revdash}.tar.gz'
    cluster().project_download(id=project['id'], revision=revision['name'], filename=filename)
    print(f'File {filename} downloaded.')
