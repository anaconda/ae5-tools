import click

from ..utils import ident_filter
from ..login import login_options, cluster_call
from ..format import format_options
from .project_collaborator import collaborator
from .project_revision import revision


@click.group(short_help='activity, collaborator, delete, deploy, deployments, download, image, info, jobs, list, patch, revision, run, runs, schedule, sessions, status, upload',
             epilog='Type "ae5 project <command> --help" for help on a specific command.')
@format_options()
@login_options()
def project():
    '''Commands related to user projects.'''
    pass


project.add_command(collaborator)
project.add_command(revision)


@project.command()
@ident_filter('project')
@click.option('--collaborators', is_flag=True, help='Include collaborators. Since this requires an API call for each project, it can be slow if there are large numbers of projects.')
@format_options()
@login_options()
def list(collaborators):
    '''List available projects.

       By default, lists all projects visible to the authenticated user.
       Simple filters on owner, project name, or id can be performed by
       supplying an optional PROJECT argument. Filters on other fields may
       be applied using the --filter option.
    '''
    cluster_call('project_list', collaborators=collaborators, cli=True)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def info(project):
    '''Retrieve information about a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_info', project, cli=True)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def jobs(project):
    '''Retrieve a list of a project's jobs.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_jobs', project, cli=True)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def runs(project):
    '''Retrieve a list of a project's runs.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_runs', project, cli=True)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def sessions(project):
    '''Retrieve a list of a project's sessions.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_sessions', project, cli=True)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def deployments(project):
    '''Retrieve a list of a project's deployments.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_deployments', project, cli=True)


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
    cluster_call('project_activity', project, limit=0 if all else limit, cli=True)


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
    cluster_call('project_patch', project, **kwargs, cli=True)


@project.command()
@click.argument('project')
@format_options()
@login_options()
def status(project):
    '''Retrieve the project's latest activity entry.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_activity', project, latest=True, cli=True)


@project.command()
@click.argument('project')
@click.option('--filename', default='', help='Filename to save to. If not supplied, the filename is constructed from the name of the project.')
@format_options()
@login_options()
@click.pass_context
def download(ctx, project, filename):
    '''Download an archive of a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       A revision value may optionally be supplied in the PROJECT identifier.
       If not supplied, the latest revision will be selected.
    '''
    from .project_revision import download as revision_download
    ctx.invoke(revision_download, revision=project, filename=filename)


@project.command()
@click.argument('project')
@click.option('--use_anaconda_cloud', is_flag=True, help='Configure Docker image to pull packages from Anaconda Cloud rather than on-premises repository.')
@click.option('--dockerfile', default='', help='Path to custom Dockerfile.')
@click.option('--debug', is_flag=True, help='Show docker image build logs.')
@login_options()
@format_options()
@click.pass_context
def image(ctx, project, use_anaconda_cloud, dockerfile, debug):
    '''Build a Docker Image of a project.

       Using the template Dockerfile the project archive is downloaded and a runable
       Docker image is built. The PROJECT identifier need not be fully specified,
       and may even include wildcards. But it must match exactly one project.

       By default the image will pull files from this AE5 Repository.

       A revision value may optionally be supplied in the PROJECT identifier.
       If not supplied, the latest revision will be selected.
    '''
    from .revision import image as revision_image
    ctx.invoke(revision_image, revision=project, use_anaconda_cloud=use_anaconda_cloud, dockerfile=dockerfile, debug=debug)
    

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
    cluster_call('project_upload', filename, name=name, tag=tag, wait=not no_wait, cli=True)


@project.command()
@click.argument('project')
@click.option('--name', type=str, required=False, help="Deployment name. If not supplied, it is autogenerated from the project name.")
@click.option('--endpoint', type=str, required=False, help='Endpoint name.')
@click.option('--command', help='The command to use for this deployment.')
@click.option('--resource-profile', help='The resource profile to use for this deployment.')
@click.option('--public', is_flag=True, help='Make the deployment public.')
@click.option('--private', is_flag=True, help='Make the deployment private (the default).')
@click.option('--wait', is_flag=True, help='Wait for the deployment to complete initialization before exiting.')
@click.option('--open', is_flag=True, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame', is_flag=True, help='Include the AE banner when opening.')
@format_options()
@login_options()
@click.pass_context
def deploy(ctx, project, name, endpoint, command, resource_profile, public, private, wait, open, frame):
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
    ctx.invoke(deployment_start, project=project, name=name, endpoint=endpoint,
               resource_profile=resource_profile, command=command,
               public=public, private=private, wait=wait, open=open, frame=frame)


@project.command()
@click.argument('project')
@click.argument('schedule')
@click.option('--name', type=str, required=False, help='Name for the job. If supplied, the name must not be identical to an existing job or run record, unless --make-unique is supplied. If not supplied, a unique name will be autogenerated from the project name.')
@click.option('--make-unique', is_flag=True, default=None, help='If supplied, a counter will be appended to a supplied --name if needed to make it unique.')
@click.option('--command', help='The command to use for this job.')
@click.option('--resource-profile', help='The resource profile to use for this job.')
@click.option('--variable', multiple=True, help='A variable setting in the form <key>=<value>. Multiple --variable options can be supplied.')
@format_options()
@login_options()
@click.pass_context
def schedule(ctx, project, schedule, command, name, make_unique, resource_profile, variable):
    '''Create a run schedule for a project.

    This command is a shortcut for the "ae5 job create" command when the intent is to
    create a scheduled run for the job. It does not run the job immediately.

    For finer control over job behavior, use "ae5 job create" instead.'''
    from .job import create as job_create
    if not schedule:
        raise click.UsageError('schedule must not be empty')
    ctx.invoke(job_create, project=project, schedule=schedule, command=command, name=name,
               resource_profile=resource_profile, variable=variable, run=False,
               make_unique=False, wait=False, show_run=False, cleanup=False)


@project.command()
@click.argument('project')
@click.option('--name', type=str, required=False, help='Name for the run. If supplied, the name must not be identical to an existing job or run record, unless --make-unique is supplied. If not supplied, a unique name will be autogenerated from the project name.')
@click.option('--make-unique', is_flag=True, default=None, help='If supplied, a counter will be appended to a supplied --name if needed to make it unique.')
@click.option('--command', help='The command to use for this job.')
@click.option('--resource-profile', help='The resource profile to use for this job.')
@click.option('--variable', multiple=True, help='A variable setting in the form <key>=<value>. Multiple --variable options can be supplied.')
@format_options()
@login_options()
@click.pass_context
def run(ctx, project, command, name, make_unique, resource_profile, variable):
    '''Execute a project as a run-once job.

    This command is a shortcut for the "ae5 job create" command when the intent is to
    run a command exactly once. It creates the job record, runs the command exactly once,
    waits for it to complete, then deletes the job record. The run record is returned.

    For finer control over job behavior, use "ae5 job create" instead.'''
    from .job import create as job_create
    ctx.invoke(job_create, project=project, schedule=None, command=command, name=name,
               resource_profile=resource_profile, variable=variable, run=True,
               make_unique=make_unique, wait=True, show_run=True, cleanup=True)


@project.command()
@click.argument('project')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@format_options()
@login_options()
def delete(project, yes):
    '''Delete a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       This command will currently fail if the project has an active session.
    '''
    cluster_call('project_delete', ident=project,
                 confirm=None if yes else 'Delete project {ident}',
                 prefix='Deleting project {ident}...',
                 postfix='deleted.', cli=True)
