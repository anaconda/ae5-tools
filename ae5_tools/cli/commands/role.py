import sys

import click

from ..login import cluster_call
from ..utils import global_options, ident_filter


@click.group(short_help="add", epilog='Type "ae5 role <command> --help" for help on a specific command.')
@global_options
def role():
    """Commands related to roles.

    These commands require the use of the KeyCloak administrator account.
    """
    pass


@role.command()
@click.option("--username", type=click.STRING, help="The username of the account to operate against.", required=True)
@click.option("--role", type=click.STRING, help="The role to add the account to.", required=True, multiple=True)
@global_options
def add(username: str, role: list):
    """Add role to user account."""

    cluster_call("user_roles_add", username=username, names=role, admin=True)


@role.command()
@click.option("--username", type=click.STRING, help="The username of the account to operate against.", required=True)
@click.option("--role", type=click.STRING, help="The role to remove from the account.", required=True, multiple=True)
@global_options
def add(username: str, role: list):
    """Remove role from user account."""

    cluster_call("user_roles_remove", username=username, names=role, admin=True)
