import click

from ..utils import ident_filter, global_options, yes_option
from ..login import cluster_call
from .project_collaborator import collaborator
from .project_revision import revision
from .deployment import start as deployment_start
from .job import _create


@click.group(short_help='activity, collaborator, delete, deploy, deployments, download, image, info, jobs, list, patch, revision, run, runs, schedule, sessions, status, upload',
             epilog='Type "ae5 project <command> --help" for help on a specific command.')
@global_options
def project():
    '''Commands related to user projects.'''
    pass


project.add_command(deployment_start, name='deploy')
project.add_command(collaborator)
project.add_command(revision)


@project.command()
@ident_filter('project')
@click.option('--collaborators', is_flag=True, help='Include collaborators. Since this requires an API call for each project, it can be slow if there are large numbers of projects.')
@global_options
def list(collaborators):
    '''List available projects.

       By default, lists all projects visible to the authenticated user.
       Simple filters on owner, project name, or id can be performed by
       supplying an optional PROJECT argument. Filters on other fields may
       be applied using the --filter option.
    '''
    cluster_call('project_list', collaborators=collaborators)


@project.command()
@ident_filter('project', required=True)
@click.option('--collaborators', is_flag=True, help='Include collaborators. Since this requires an API call for each project, it can be slow if there are large numbers of projects.')
@global_options
def info(collaborators):
    '''Retrieve information about a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_info', collaborators=collaborators)


@project.command()
@ident_filter('project', required=True)
@global_options
def jobs():
    '''Retrieve a list of a project's jobs.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_jobs')


@project.command()
@ident_filter('project', required=True)
@global_options
def runs():
    '''Retrieve a list of a project's runs.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_runs')


@project.command()
@ident_filter('project', required=True)
@global_options
def sessions():
    '''Retrieve a list of a project's sessions.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_sessions')


@project.command()
@ident_filter('project', required=True)
@global_options
def deployments():
    '''Retrieve a list of a project's deployments.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_deployments')


@project.command()
@ident_filter('project', required=True)
@click.option('--limit', type=int, default=None, help='Limit the output to N records.')
@click.option('--all', is_flag=True, help='Retrieve all possible records.')
@click.option('--latest', is_flag=True, help='Return only the latest record.')
@global_options
def activity(**kwargs):
    '''Retrieve the project's acitivty log.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       By default, the latest 10 records will be returned. This behavior can be
       adjusted using the --limit or --all options.
    '''
    cluster_call('project_activity', **kwargs)


@project.command()
@ident_filter('project', required=True)
@click.option('--name', help='A new name for the project.')
@click.option('--editor', help='The editor to use for future sessions.')
@click.option('--resource-profile', help='The resource profile to use for future sessions.')
@global_options
def patch(**kwargs):
    '''Change the project's name, editor, or resource profile.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_patch', **kwargs)


@project.command()
@ident_filter('project', required=True)
@global_options
def status():
    '''Retrieve the project's latest activity entry.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    cluster_call('project_activity', latest=True)


@project.command()
@ident_filter('project', required=True, handle_revision=True)
@click.option('--filename', default='', help='Filename to save to. If not supplied, the filename is constructed from the name of the project.')
@global_options
def download(**kwargs):
    '''Download an archive of a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       A revision value may optionally be supplied in the PROJECT identifier.
       If not supplied, the latest revision will be selected.
    '''
    from .project_revision import _download
    _download(**kwargs)


@project.command()
@ident_filter('project', required=True, handle_revision=True)
@click.option('--command', default='', help='Command name to execute.')
@click.option('--condarc', default='', help='Path to custom condarc file.')
@click.option('--dockerfile', default='', help='Path to custom Dockerfile.')
@click.option('--debug', is_flag=True, help='debug logs')
@global_options
def image(**kwargs):
    '''Build a Docker Image of a project.

       Using the template Dockerfile the project archive is downloaded and a runable
       Docker image is built. The PROJECT identifier need not be fully specified,
       and may even include wildcards. But it must match exactly one project.

       By default the image will pull files from this AE5 Repository.

       A revision value may optionally be supplied in the PROJECT identifier.
       If not supplied, the latest revision will be selected.
    '''
    from .project_revision import _image
    _image(**kwargs)


@project.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('--name', default='', help='Name of the project.')
@click.option('--tag', default='', help='Commit tag to use for initial revision of project.')
@click.option('--no-wait', is_flag=True, help='Do not wait for the creation session to complete before exiting.')
@global_options
def upload(filename, name, tag, no_wait):
    '''Upload a project.

       By default, the name of the project is taken from the basename of
       the file. This can be overridden by using the --name option. The
       name must not be the same as an existing project.
    '''
    cluster_call('project_upload', filename, name=name, tag=tag, wait=not no_wait)


@project.command()
@ident_filter('project', required=True, handle_revision=True)
@click.argument('schedule')
@click.option('--name', type=str, required=False, help='Name for the job. If supplied, the name must not be identical to an existing job or run record, unless --make-unique is supplied. If not supplied, a unique name will be autogenerated from the project name.')
@click.option('--make-unique', is_flag=True, default=None, help='If supplied, a counter will be appended to a supplied --name if needed to make it unique.')
@click.option('--command', help='The command to use for this job.')
@click.option('--resource-profile', help='The resource profile to use for this job.')
@click.option('--variable', multiple=True, help='A variable setting in the form <key>=<value>. Multiple --variable options can be supplied.')
@global_options
def schedule(**kwargs):
    '''Create a run schedule for a project.

    This command is a shortcut for the "ae5 job create" command when the intent is to
    create a scheduled run for the job. It does not run the job immediately.

    For finer control over job behavior, use "ae5 job create" instead.'''
    _create(**kwargs)


@project.command()
@ident_filter('project', required=True, handle_revision=True)
@click.option('--name', type=str, required=False, help='Name for the run. If supplied, the name must not be identical to an existing job or run record, unless --make-unique is supplied. If not supplied, a unique name will be autogenerated from the project name.')
@click.option('--make-unique', is_flag=True, default=None, help='If supplied, a counter will be appended to a supplied --name if needed to make it unique.')
@click.option('--command', help='The command to use for this job.')
@click.option('--resource-profile', help='The resource profile to use for this job.')
@click.option('--variable', multiple=True, help='A variable setting in the form <key>=<value>. Multiple --variable options can be supplied.')
@global_options
def run(**kwargs):
    '''Execute a project as a run-once job.

    This command is a shortcut for the "ae5 job create" command when the intent is to
    run a command exactly once. It creates the job record, runs the command exactly once,
    waits for it to complete, then deletes the job record. The run record is returned.

    For finer control over job behavior, use "ae5 job create" instead.'''
    kwargs['run'] = kwargs['wait'] = kwargs['show_run'] = kwargs['cleanup'] = True
    _create(**kwargs)


@project.command()
@ident_filter('project', required=True)
@yes_option
@global_options
def delete(**kwargs):
    '''Delete a project.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       This command will currently fail if the project has an active session.
    '''
    cluster_call('project_delete', **kwargs,
                 confirm='Delete project {ident}',
                 prefix='Deleting project {ident}...',
                 postfix='deleted.')


@project.command()
@ident_filter('project', required=True)
@click.argument('directory', type=str, required=False, default="")
@click.option('--use-https', is_flag=True, default=None, required=False,
                help="When using external git (i.e., Github.com) clone will use SSH. Set this flag to use HTTPs instead.")
@global_options
def clone(directory, use_https):
    '''Clone a project as a local git clone.

       The PROJECT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.

       If the local directory name is not supplied the project will be cloned
       according to the repository field from "ae5 project info.

       The project id is added to .git/config as remote.origin.project.
    '''
    cluster_call('project_clone', directory, use_https)
