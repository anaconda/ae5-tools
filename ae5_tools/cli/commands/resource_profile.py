import click

from ..login import login_options, cluster_call
from ..format import print_output, format_options


@click.group(short_help='info, list',
             epilog='Type "ae5 resource-profile <command> --help" for help on a specific command.')
@format_options()
@login_options()
def resource_profile():
    '''Commands related to resource profiles.'''
    pass


@resource_profile.command()
@format_options()
@login_options()
def list():
    '''List all availables resource profiles.
    '''
    cluster_call('resource_profile_list', cli=True)


@resource_profile.command()
@click.argument('name')
@format_options()
@login_options()
def info(name):
    '''Retrieve the record of a single resource profile.

       The NAME identifier must match exactly one name of a resource profile.
       Wildcards may be included.
    '''
    cluster_call('resource_profile_info', name, cli=True)
