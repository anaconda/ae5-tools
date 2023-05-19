import click

from ..login import cluster_call
from ..utils import global_options, ident_filter, yes_option


@click.group(
    short_help="delete, info, list, log, stop", epilog='Type "ae5 run <command> --help" for help on a specific command.'
)
@global_options
def run():
    """Commands related to run records."""
    pass


@run.command()
@ident_filter("run")
@click.option("--k8s", is_flag=True, help="Include Kubernetes-derived columns (requires additional API calls).")
@global_options
def list(**kwargs):
    """List all available run records.

    By default, lists all runs visible to the authenticated user.
    Simple filters on owner, run name, or id can be performed by
    supplying an optional RUN argument. Filters on other fields may
    be applied using the --filter option.
    """
    cluster_call("run_list", **kwargs)


@run.command()
@ident_filter("run", required=True)
@click.option("--k8s", is_flag=True, help="Include Kubernetes-derived columns (requires additional API calls).")
@global_options
def info(**kwargs):
    """Retrieve information about a single run.

    The RUN identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one run.
    """
    cluster_call("run_info", **kwargs)


@run.command(short_help="Retrieve the log for a single run.")
@ident_filter("run", required=True)
@global_options
def log(**kwargs):
    """Retrieve the log file for a particular run.

    The RUN identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one run.
    """
    cluster_call("run_log", **kwargs)


@run.command()
@ident_filter("run", required=True)
@yes_option
@global_options
def stop(**kwargs):
    """Stop a run.

    Does not produce an error if the run has already completed.

    The RUN identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one run.
    """
    cluster_call("run_stop", **kwargs, confirm="Stop run {ident}", prefix="Stopping run {ident}...", postfix="stopped.")


@run.command()
@ident_filter("run", required=True)
@yes_option
@global_options
def delete(**kwargs):
    """Delete a run record.

    The RUN identifier need not be fully specified, and may even include
    wildcards. But it must match exactly one run.
    """
    cluster_call(
        "run_delete", **kwargs, confirm="Delete run {ident}", prefix="Deleting run {ident}...", postfix="deleted."
    )
