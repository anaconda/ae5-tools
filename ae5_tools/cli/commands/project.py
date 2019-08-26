import click

from ..utils import add_param, ident_filter
from ..login import login_options, cluster_call
from ..format import print_output, format_options
from ...identifier import Identifier
from .project_collaborator import collaborator
from .project_revision import revision


@click.group(short_help='activity, collaborator, delete, deploy, deployments, info, jobs, list, patch, revision, runs, sessions, status, upload',
             epilog='Type "ae5 project <command> --help" for help on a specific command.')
@format_options()
@login_options()
def project():
    pass


project.add_command(collaborator)
project.add_command(revision)


@project.command()
@ident_filter('project')
@format_options()
@login_options()
def list():
    '''List available projects.

       By default, lists all projects visible to the authenticated user.
       Simple filters on owner, project name, or id can be performed by
       supplying an optional PROJECT argument. Filters on other fields may
       be applied using the --filter option.
    '''
    cluster_call('project_list', cli=True)


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
@click.option('--endpoint', type=str, required=False, help='Endpoint name.')
@click.option('--command', help='The command to use for this deployment.')
@click.option('--resource-profile', help='The resource profile to use for this deployment.')
@click.option('--public', is_flag=True, help='Make the deployment public.')
@click.option('--private', is_flag=True, help='Make the deployment private (the default).')
@click.option('--wait/--no-wait', default=True, help='Wait for the deployment to complete initialization before exiting.')
@click.option('--open/--no-open', default=True, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame/--no-frame', default=False, help='Include the AE banner when opening.')
@format_options()
@login_options()
@click.pass_context
def deploy(ctx, project, endpoint, command, resource_profile, public, private, wait, open, frame):
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
               resource_profile=resource_profile, command=command,
               public=public, private=private, wait=wait, open=open, frame=frame)


@project.command()
@click.argument('project')
@click.argument('schedule')
@click.option('--name', type=str, required=False, help="Name for the job. If not supplied, it is autogenerated from the project name.")
@click.option('--command', help='The command to use for this job.')
@click.option('--resource-profile', help='The resource profile to use for this job.')
@click.option('--variable', multiple=True, help='A variable setting in the form <key>=<value>. Multiple --variable options can be supplied.')
@format_options()
@login_options()
@click.pass_context
def schedule(ctx, project, schedule, command, name, resource_profile, variable):
    '''Create a run schedule for a project.

    This command is a shortcut for the "ae5 job create" command when the intent is to
    create a scheduled run for the job. It does not run the job immediately.

    For finer control over job behavior, use "ae5 job create" instead.'''
    from .job import create as job_create
    if not schedule:
        raise click.UsageError('schedule must not be empty')
    ctx.invoke(job_create, project=project, schedule=schedule, command=command, name=name,
               resource_profile=resource_profile, variable=variable, run=False,
               wait=False, show_run=False, cleanup=False)


@project.command()
@click.argument('project')
@click.option('--name', type=str, required=False, help="Name for the run. If not supplied, it is autogenerated from the project name.")
@click.option('--command', help='The command to use for this job.')
@click.option('--resource-profile', help='The resource profile to use for this job.')
@click.option('--variable', multiple=True, help='A variable setting in the form <key>=<value>. Multiple --variable options can be supplied.')
@format_options()
@login_options()
@click.pass_context
def run(ctx, project, command, name, resource_profile, variable):
    '''Execute a project as a run-once job.

    This command is a shortcut for the "ae5 job create" command when the intent is to
    run a command exactly once. It creates the job record, runs the command exactly once,
    waits for it to complete, then deletes the job record. The run record is returned.

    For finer control over job behavior, use "ae5 job create" instead.'''
    from .job import create as job_create
    ctx.invoke(job_create, project=project, schedule=None, command=command, name=name,
               resource_profile=resource_profile, variable=variable, run=True,
               wait=True, show_run=True, cleanup=True)


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
    cluster_call('project_delete', result['id'],
                 confirm=None if yes else f'Delete project {ident}',
                 prefix=f'Deleting project {ident}...',
                 postfix='deleted.', cli=True)
