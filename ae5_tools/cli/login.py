import click

from ..config import config
from ..api import AECluster
from .utils import param_callback


_login_options = [
    click.option('--hostname', type=str, expose_value=False, callback=param_callback, envvar='AE5_HOSTNAME',
                 help='The hostname of the cluster to connect to.'),
    click.option('--username', type=str, expose_value=False, callback=param_callback, envvar='AE5_USERNAME',
                 help='The username to use for authentication.'),
    click.option('--password', type=str, expose_value=False, callback=param_callback, envvar='AE5_PASSWORD',
                 help='The password to use for authentication.')
]


def login_options(password=True):
    def apply(func):
        for option in reversed(_login_options[:2 + password]):
            func = option(func)
        return func
    return apply


def get_account(required=False):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    hostname = obj.get('hostname')
    username = obj.get('username')
    if not hostname and not username:
        matches = config.default()
        matches = [matches] if matches else []
    else:
        matches = config.resolve(hostname, username)
    if len(matches) == 1:
        hostname, username = matches[0]
    elif required:
        ask = obj.get('is_interactive')
        if hostname or username:
            if len(matches) == 0:
                msg = 'No saved sessions match'
            else:
                msg = 'Multiple saved accounts match'
            if hostname:
                msg += f' hostname "{hostname}"'
            if hostname and username:
                msg += ' and'
            if username:
                msg += f' username "{username}"'
            if obj.get('is_interactive'):
                click.echo(msg)
            else:
                click.UsageError(msg)
        elif not ask:
            click.UsageError('Must supply username and hostname')
        hostname = click.prompt('Hostname', default=hostname, type=str)
        username = click.prompt('Username', default=username, type=str)
    return hostname, username


def cluster(reconnect=False):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    if 'cluster' not in obj or reconnect:
        hostname, username = get_account(required=True)
        obj['cluster'] = AECluster(hostname, username, obj.get('password'))
    return obj['cluster']


def cluster_call(method, *args, **kwargs):
    try:
        return getattr(cluster(), method)(*args, **kwargs)
    except Exception as e:
        raise
        raise click.UsageError(str(e))
