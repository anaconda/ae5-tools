import click

from ..login import cluster_call
from ..utils import global_options, yes_option


@click.group(short_help="list, add, delete",
             epilog='Type "ae5 secret <command> --help" for help on a specific command.')
@global_options
def secret():
    """Commands related to managing user secrets."""
    pass


@secret.command()
@global_options
def list(**kwargs):
    """List all user secrets."""
    cluster_call("secret_list", **kwargs)


@secret.command()
@click.argument("key")
@yes_option
@global_options
def delete(key, **kwargs):
    """Delete a secret.
       The secret key must match exactly.
    """
    cluster_call("secret_delete", key=key, **kwargs,
                 confirm=f"Delete secret {key}",
                 prefix=f"Deleting secret {key}...",
                 postfix="deleted.")


@secret.command()
@click.argument("key", type=str)
@click.argument("value")
@yes_option
@global_options
def add(key, value, **kwargs):
    """Upsert (add or update) a secret.
       Must provide key and value as arguments.
       If performing an update it may take several minutes before the change is active.
    """
    cluster_call("secret_add", key=key, value=value, **kwargs,
                 prefix=f"Adding secret {key}...",
                 postfix="added.")
