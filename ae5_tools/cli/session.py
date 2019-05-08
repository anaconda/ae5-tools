import click

from .utils import cluster, filter_df, sort_df, print_output, format_options, Identifier, add_param


@click.group()
def session():
    pass


@session.command()
@click.argument('session', required=False)
@format_options()
def list(session):
    result = cluster().sessions(format='dataframe')
    if session:
        add_param('filter', Identifier.from_string(session).project_filter())
    print_output(result)


def single_session(session):
    ident = Identifier.from_string(session)
    result = cluster().sessions(format='dataframe')
    result = filter_df(result, ident.project_filter())
    if len(result) == 0:
        raise click.UsageError(f'Session not found: {session}')
    elif len(result) > 1:
        raise click.UsageError(f'Multiple sessions found matcing {session}; specify owner?')
    return result.astype(object).iloc[0]


@session.command()
@click.argument('session')
@format_options()
def info(session):
    result = single_session(session)
    print_output(result)

