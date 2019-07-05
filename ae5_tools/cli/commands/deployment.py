import click
import webbrowser
import re

from ..login import cluster_call, login_options
from ..utils import add_param
from ..format import print_output, format_options
from ...identifier import Identifier


@click.group(short_help='list, info, endpoints, collaborators, start, stop',
             epilog='Type "ae5 deployment <command> --help" for help on a specific command.')
@format_options()
@login_options()
def deployment():
    pass


@deployment.command()
@click.argument('deployment', required=False)
@click.option('--collaborators', is_flag=True, help='Include the list of collaborators. This adds a separate API call for each project, so for performance reasons it is off by default.')
@format_options()
@login_options()
def list(deployment):
    '''List available projects.

       By default, lists all projects visible to the authenticated user.
       Simple filters on owner, project name, or id can be performed by
       supplying an optional DEPLOYMENT argument. Filters on other fields may
       be applied using the --filter option.
    '''
    result = cluster_call('deployment_list', collaborators=collaborators, format='dataframe')
    if deployment:
        add_param('filter', Identifier.from_string(deployment).project_filter())
    print_output(result)


def single_deployment(deployment, collaborators=False):
    ident = Identifier.from_string(deployment)
    return cluster_call('deployment_info', ident, collaborators=collaborators, format='dataframe')


@deployment.command()
@format_options()
@login_options()
def endpoints():
    '''List all static endpoints.'''
    result = cluster_call('deployment_endpoints', format='dataframe')
    print_output(result)


@deployment.command()
@click.argument('deployment')
@click.option('--collaborators', is_flag=True, help='Include the list of collaborators. This adds a separate API call for each project, so for performance reasons it is off by default.')
@format_options()
@login_options()
def info(deployment, collaborators):
    '''Obtain information about a single deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = single_deployment(deployment, collaborators)
    print_output(result)


@deployment.command()
@click.argument('deployment')
@click.option('--public', is_flag=True, help='Set the deployment to public.')
@click.option('--private', is_flag=True, help='Set the deployment to private.')
@format_options()
@login_options()
def patch(deployment, public, private):
    '''Change the deployment's public/private status.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one deployment.
    '''
    if public and private:
        click.ClickException('Cannot specify both --public and --private')
    if not public and not private:
        public = None
    result = cluster_call('deployment_patch', deployment, public=public, format='dataframe')
    print_output(result)


@deployment.command(short_help='Obtain information about a deployment\'s collaborators.')
@click.argument('deployment')
@format_options()
@login_options()
def collaborators(deployment):
    '''Obtain information about a deployment's collaborators.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = cluster_call('deployment_collaborators', deployment, format='dataframe')
    print_output(result)


def _open(record, frame):
    scheme, _, hostname, _ = record.project_url.split('/', 3)
    if frame:
        url = f'{scheme}//{hostname}/deployments/detail/{record.id}/view'
    else:
        url = record.url
    webbrowser.open(url, 1, True)


@deployment.command(short_help='Start a deployment for a project.')
@click.argument('project')
@click.option('--endpoint', type=str, required=False, help='Endpoint name.')
@click.option('--command', help='The command to use for this deployment.')
@click.option('--resource-profile', help='The resource profile to use for this deployment.')
@click.option('--public', is_flag=True, help='Make the deployment public.')
@click.option('--private', is_flag=True, help='Make the deployment private (the default).')
@click.option('--wait/--no-wait', default=True, help='Wait for the deployment to complete initialization before exiting.')
@click.option('--open/--no-open', default=False, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame/--no-frame', default=False, help='Include the AE banner when opening.')
@format_options()
@login_options()
@click.pass_context
def start(ctx, project, endpoint, command, resource_profile, public, private, wait, open, frame):
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
    if public and private:
        click.ClickException('Cannot specify both --public and --private')
    prec = cluster_call('project_info', project, format='json')
    endpoints = cluster_call('deployment_endpoints', format='json')
    e_supplied = bool(endpoint)
    if not e_supplied:
        dedupe = True
        for e in endpoints:
            if e['project_id'] == prec['id']:
                if not e['deployment_id']:
                    endpoint = e['id']
                    dedupe = False
                    break
                elif not endpoint:
                    endpoint = e['id']
        if dedupe:
            if not endpoint:
                endpoint = re.sub(r'[^A-Za-z0-9-]', '', re.sub(r'[_\s]', '-', prec['name'])).lower()
            count = -1
            base_endpoint = endpoint
            while any(e['id'] == endpoint for e in endpoints):
                endpoint = f'{base_endpoint}{count}'
                count -= 1
    else:
        if not re.match(r'[A-Za-z0-9-]+', endpoint):
            click.ClickException(f'Invalid endpoint: {endpoint}')
        for e in endpoints:
            if e['id'] == endpoint:
                if e['project_id'] == prec['id']:
                    if e['deployment_id']:
                        click.ClickException(f'Endpoint {endpoint} is already active for this project')
                    else:
                        break
                elif prec['name']:
                    click.ClickException(f'Endpoint {endpoint} is claimed by project {prec["owner"]/prec["name"]}')
                elif e['owner'] != prec['owner']:
                    click.ClickException(f'Endpoint {endpoint} is claimed by another user')
                else:
                    click.ClickException(f'Endpoint {endpoint} is claimed by another project')
    ident = Identifier.from_record(prec)
    click.echo(f'Starting deployment {endpoint} for {ident}...', nl=False, err=True)
    response = cluster_call('deployment_start', ident, endpoint=endpoint, command=command,
                            resource_profile=resource_profile, public=public,
                            wait=wait or open, format='dataframe')
    click.echo('restarted.', err=True)
    if open:
        _open(response, frame)
    print_output(response)


@deployment.command(short_help='Start a deployment for a project.')
@click.argument('deployment')
@click.option('--wait/--no-wait', default=True, help='Wait for the deployment to complete initialization before exiting.')
@click.option('--open/--no-open', default=False, help='Open a browser upon initialization. Implies --wait.')
@click.option('--frame/--no-frame', default=False, help='Include the AE banner when opening.')
@format_options()
@login_options()
@click.pass_context
def restart(ctx, deployment, wait, open, frame):
    '''Restart a deployment for a project.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    drec = cluster_call('deployment_info', deployment, format='json')
    obj = ctx.ensure_object(dict)
    if drec['owner'] != obj['username']:
        ident = Identifier.from_record(drec)
        msg = f'user {obj["username"]} cannot restart deployment {ident}'
        raise click.ClickException(msg)
    click.echo(f'Restarting deployment {ident}...', nl=False, err=True)
    response = cluster_call('deployment_restart', drec['id'], wait=wait or open, format='dataframe')
    click.echo('restarted.', err=True)
    if open:
        _open(response, frame)
    print_output(response)


@deployment.command(short_help='Stop a deployment.')
@click.argument('deployment')
@click.option('--yes', is_flag=True, help='Do not ask for confirmation.')
@login_options()
def stop(deployment, yes):
    '''Stop a deployment.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one project.
    '''
    result = single_deployment(deployment)
    ident = Identifier.from_record(result)
    if not yes:
        yes = click.confirm(f'Stop deployment {ident}', err=True)
    if yes:
        click.echo(f'Stopping {ident}...', nl=False, err=True)
        cluster_call('deployment_stop', result.id)
        click.echo('stopped.', err=True)


@deployment.command(short_help='Open a deployment in a browser.')
@click.argument('deployment')
@click.option('--frame/--no-frame', default=False, help='Include the AE banner.')
@login_options()
def open(deployment, frame):
    '''Opens a deployment in the default browser.

       The DEPLOYMENT identifier need not be fully specified, and may even include
       wildcards. But it must match exactly one session.

       For deployments, the frameless version of the deployment will be opened by
       default. If you wish to the Anaconda Enterprise banner at the top
       of the window, use the --frame option.
    '''
    result = single_deployment(deployment)
    _open(result, frame)
