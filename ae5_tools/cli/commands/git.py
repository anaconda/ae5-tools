import click

from ..utils import ident_filter, global_options, yes_option
from ..login import cluster_call
from .project_collaborator import collaborator
from .project_revision import revision
from .deployment import start as deployment_start
from .job import _create


@click.group(short_help='config',
             epilog='Type "ae5 git <command> --help" for help on a specific command.')
@global_options
def git():
    '''Commands related to user projects.'''
    pass

@git.command()
@ident_filter('git')
@global_options
def config():
    '''Configure git http.extraheader.

    '''
    cluster_call('git_config')

@git.command()
@ident_filter('git')
@global_options
def install_prepush():
    '''Setup .git/hooks/pre-push to enable metadata POST for tagged commits.'''
    cluster_call('git_install_prepush')

@click.group(short_help='Commands related to git push')
@global_options
def post():
    '''Commands related to user projects.'''
    pass

@post.command()
@ident_filter('post')
@global_options
def revision_metadata():
    '''POST revision metadata.'''
    cluster_call('post_revision_metadata')
