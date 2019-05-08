import click

from .utils import cluster, filter_df, sort_df, print_output, format_options, Identifier, add_param


@click.group()
def project():
    pass


@project.command()
@click.argument('project', required=False)
@format_options()
def list(project):
    result = cluster().projects(format='dataframe')
    if project:
        add_param('filter', Identifier.from_string(project).project_filter())
    print_output(result)


def single_project(project):
    ident = Identifier.from_string(project)
    result = cluster().projects(format='dataframe')
    result = filter_df(result, ident.project_filter())
    if len(result) == 0:
        raise click.UsageError(f'Project not found: {project}')
    elif len(result) > 1:
        raise click.UsageError(f'Multiple projects found matcing {project}; specify owner?')
    return result.astype(object).iloc[0]


@project.command()
@click.argument('project')
@format_options()
def info(project):
    result = single_project(project)
    print_output(result)


@project.command()
@click.argument('project')
@click.option('--filename', default='', help='Filename to save to. If not supplied, the filename is constructed from the name of the project.')
@click.pass_context
def download(ctx, project, filename):
    if ':' not in project:
        project += ':latest'
    from .revision import download as revision_download
    ctx.invoke(revision_download, revision=project, filename=filename)


@project.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--name', default='', help='Name of the project. If not supplied, it will be taken from the basename of the file.')
def upload(filename, name):
    print(f'Uploading {filename}...')
    cluster().project_upload(filename, name=name or None, wait=True)
    print('done.')


@project.command()
@click.argument('project')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
def delete(project, yes):
    result = single_project(project)
    ident = f'{result["owner"]}/{result["name"]}/{result["id"]}'
    if not yes:
        yes = click.confirm(f'Delete project {ident}')
    if yes:
        click.echo(f'Deleting {ident}...', nl=False)
        cluster().project_delete(result.id)
        click.echo('done.')
