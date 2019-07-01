import click

from ..utils import add_param
from ..login import login_options, cluster_call
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='list, info, download, upload, deploy, deployments, jobs, runs, activity, status, delete',
             epilog='Type "ae5 project <command> --help" for help on a specific command.')
@format_options()
@login_options()
def project():
    pass


@project.command()
@click.argument('project', required=False)
@click.option('--collaborators', is_flag=True, help='Include the list of collaborators. This adds a separate API call for each project, so for performance reasons it is off by default.')
@format_options()
@login_options()
def list(project, collaborators):
    '''List available projects.

       By default, lists all projects visible to the authenticated user.
       Simple filters on owner, project name, or id can be performed by
       supplying an optional PROJECT argument. Filters on other fields may
       be applied using the --filter option.
    '''
    result = cluster_call('project_list', collaborators=collaborators, format='dataframe')
    if project:
        add_param('filter', Identifier.from_string(project).project_filter())
    print_output(result)


@project.command()
@click.argument('project', required=False)
@format_options()
@login_options()
def samples(project):
    '''List sample projects.
    '''
    result = cluster_call('project_samples', format='dataframe')
    if project:
        add_param('filter', Identifier.from_string(project).project_filter())
    print_output(result)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def info(project):
    '''Obtain information about a single project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_info', project, collaborators=True, format='dataframe')
    print_output(result)


@project.command()
@click.argument('name_or_id')
@format_options()
@login_options()
def sample_info(name_or_id):
    '''Obtain information about a single sample project.

       The NAME_OR_ID identifier may include wildcards but it must match exactly one sample.
    '''
    result = cluster_call('project_sample_info', name_or_id, format='dataframe')
    print_output(result)


@project.command()
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


@project.command()
@click.argument('project')
@format_options()
@login_options()
def jobs(project):
    '''Obtain information about a project's jobs.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_jobs', project, format='dataframe')
    print_output(result)


@project.command(short_help='Retrieve the project\'s runs.')
@click.argument('project')
@format_options()
@login_options()
def runs(project):
    '''Obtain information about a project's runs.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_collaborators', project, format='dataframe')
    print_output(result)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def deployments(project):
    '''Obtain information about a project's deployments.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('project_deployments', project, format='dataframe')
    print_output(result)


@project.command()
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


@project.command()
@click.argument('project')
@click.option('--name', help='A new name for the project.')
@click.option('--editor', help='The editor to use for future sessions.')
@click.option('--resource-profile', help='The resource profile to use for future sessions.')
@format_options()
@login_options()
def patch(project, **kwargs):
    '''Change the project's name, editor, or resource profile.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    kwargs['format'] = 'dataframe'
    result = cluster_call('project_patch', project, **kwargs)
    print_output(result)


@project.command()
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


@project.command()
@click.argument('project')
@click.option('--filename', default='', help='Filename to save to. If not supplied, the filename is constructed from the name of the project.')
@login_options()
@click.pass_context
def download(ctx, project, filename):
    '''Download an archive of a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       A revision value may optionally be supplied in the PROJECT identifier.
       If not supplied, the latest revision will be selected.
    '''
    from .revision import download as revision_download
    ctx.invoke(revision_download, revision=project, filename=filename)


@project.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--name', default='', help='Name of the project.')
@click.option('--tag', default='', help='Commit tag to use for initial revision of project.')
@click.option('--no-wait', is_flag=True, help='Do not wait for the creation seesion to complete before exiting.')
@format_options()
@login_options()
def upload(filename, name, tag, no_wait):
    '''Upload a project.

       By default, the name of the project is taken from the basename of
       the file. This can be overridden by using the --name option. The
       name must not be the same as an existing project.
    '''
    result = cluster_call('project_upload', filename, name=name, tag=tag, wait=not no_wait, format='dataframe')
    print_output(result)


@project.command()
@click.argument('project')
@click.option('--endpoint', type=str, required=False, help='Endpoint name.')
@click.option('--resource-profile', help='The resource profile to use for this deployment.')
@click.option('--wait/--no-wait', default=True, help='Wait for the deployment to complete initialization before exiting.')
@click.option('--open/--no-open', default=True, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame/--no-frame', default=False, help='Include the AE banner when opening.')
@format_options()
@login_options()
@click.pass_context
def deploy(ctx, project, endpoint, resource_profile, wait, open, frame):
    '''Start a deployment for a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       If the static endpoint is supplied, it must be of the form r'[A-Za-z0-9-]+',
       and it will be converted to lowercase. It must not match any endpoint with
       an active deployment, nor can it match any endpoint claimed by another project,
       even if that project has no active deployments. If the endpoint is not supplied,
       it will be autogenerated from the project name.

       By default, this command will wait for the completion of the deployment
       creation before returning. To return more quickly, use the --no-wait option.
    '''
    from .deployment import start as deployment_start
    ctx.invoke(deployment_start, project=project, endpoint=endpoint,
               resource_profile=resource_profile, wait=wait, open=open, frame=frame)


@project.command()
@click.argument('project')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def delete(project, yes):
    '''Delete a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       This command will currently fail if the project has an active session.
    '''
    result = cluster_call('project_info', project, format='json')
    ident = Identifier.from_record(result)
    if yes:
        click.echo(f'Deleting project {ident}...')
    else:
        yes = click.confirm(f'Delete project {ident}')
    if yes:
        result = cluster_call('project_delete', result['id'])
