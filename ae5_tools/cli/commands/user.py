import sys

import click

from ..login import cluster_call
from ..utils import global_options, ident_filter


@click.group(
    short_help="info, list, create, delete", epilog='Type "ae5 user <command> --help" for help on a specific command.'
)
@global_options
def user():
    """Commands related to user accounts.

    These commands require the use of the KeyCloak administrator account.
    """
    pass


@user.command()
@ident_filter("username", "username={value}|id={value}")
@global_options
def list():
    """List all users."""
    cluster_call("user_list", admin=True)


@user.command()
@ident_filter("username", "username={value}|id={value}", required=True)
@global_options
def info():
    """Retrieve information about a single user.

    USERNAME must exactly match either one username or one KeyCloak user ID."""
    cluster_call("user_info", admin=True)


@user.command()
@click.argument("param", nargs=-1)
@click.option("--limit", type=click.IntRange(1), default=sys.maxsize, help="The maximum number of events to return.")
@click.option("--first", type=click.IntRange(0), default=0, help="The index of the first element to return.")
@global_options
def events(param, limit, first):
    """Retrieve KeyCloak events.

    Each PARAM argument must be of the form <key>=<value>.
    """
    param = [z.split("=", 1) for z in param]
    param = dict((x.rstrip(), y.lstrip()) for x, y in param)
    cluster_call("user_events", limit=limit, first=first, **param, admin=True)


@user.command()
@click.option("--username", type=click.STRING, help="The username of the new account.", required=True)
@click.option("--email", type=click.STRING, help="The email address of the new account.", required=True)
@click.option("--firstname", type=click.STRING, help="The first name of the new account.", required=True)
@click.option("--lastname", type=click.STRING, help="The last name of the new account.", required=True)
@click.option("--enabled", type=click.BOOL, help="Whether to enable the account on creation.", required=True)
@click.option("--email-verified", type=click.BOOL, help="Whether the email address was verified.", required=True)
@click.option("--password", type=click.STRING, help="The password of the new account.", required=True)
@click.option(
    "--password-temporary", type=click.BOOL, help="Whether the provided password is temporary.", required=True
)
@global_options
def create(
    username: str,
    email: str,
    firstname: str,
    lastname: str,
    enabled: bool,
    email_verified: bool,
    password: str,
    password_temporary: bool,
):
    """Create a new user account."""

    cluster_call(
        "user_create",
        username=username,
        email=email,
        firstname=firstname,
        lastname=lastname,
        enabled=enabled,
        email_verified=email_verified,
        password=password,
        password_temporary=password_temporary,
        admin=True,
    )


@user.command()
@click.option("--username", type=click.STRING, help="The username of the account to delete.", required=True)
@global_options
def delete(username: str):
    """Delete a new user account."""

    cluster_call("user_delete", username=username, admin=True)
