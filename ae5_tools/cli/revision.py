import click

from .utils import filter_df, print_output, format_options, Identifier
from .project import single_project


@click.group()
@click.pass_context
def revision(ctx):
    pass


@revision.command()
@click.argument('project')
@format_options()
@click.pass_context
def list(ctx, project, filter, sort, format, width, wide, header):
    id = single_project(ctx, project).id
    result = ctx.obj['cluster'].project_revisions(id=id, format='dataframe')
    print_output(result, filter, sort, format, width, wide, header)


def single_revision(ctx, revision, strict=True):
    ident = revision if isinstance(revision, Identifier) else Identifier.from_string(revision)
    project = single_project(ctx, ident.to_string(drop_revision=True))
    result = ctx.obj['cluster'].project_revisions(id=project['id'], format='dataframe')
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
@format_options(single=True)
@click.pass_context
def info(ctx, revision, filter, sort, format, width, wide, header):
    revision, project = single_revision(ctx, revision, strict=False)
    print_output(revision, filter, sort, format, width, wide, header)


@revision.command()
@click.argument('revision')
@click.option('--filename', default='', help='Filename')
@click.pass_context
def download(ctx, revision, filename):
    revision, project = single_revision(ctx, revision)
    if not filename:
        filename = project['name'] + '.tar.gz'
    ctx.obj['cluster'].project_download(id=project['id'], filename=filename)
    print(f'File {filename} downloaded.')
