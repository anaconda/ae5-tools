import click

from ..utils import add_param
from ..login import login_options, cluster_call
from ..format import print_output, format_options


@click.group(short_help='info, list',
             epilog='Type "ae5 project sample <command> --help" for help on a specific command.')
@format_options()
@login_options()
def sample():
    pass


@sample.command()
@format_options()
@login_options()
def list():
    '''List the sample projects.
    '''
    result = cluster_call('sample_list', format='dataframe')
    print_output(result)


@sample.command()
@click.argument('project')
@format_options()
@login_options()
def info(project):
    '''Retrieve the record of a single sample project.

       The PROJECT identifier must match exactly one name or id of a sample project.
       Wildcards may be included.

       USERID must be an exact match of the user ID of an individual, or the name
       of a group (e.g., 'everyone').
    '''
    result = cluster_call('sample_info', project, format='dataframe')
    print_output(result)
