import click

from ..config import config
from ..api import AEUserSession, AEAdminSession
from .utils import param_callback


_login_options = [
    click.option('--hostname', type=str, expose_value=False, callback=param_callback, envvar='AE5_HOSTNAME',
                 help='The hostname of the cluster to connect to.'),
    click.option('--username', type=str, expose_value=False, callback=param_callback, envvar='AE5_USERNAME',
                 help='The username to use for authentication.'),
    click.option('--password', type=str, expose_value=False, callback=param_callback, envvar='AE5_PASSWORD',
                 help='The password to use for authentication.'),
    click.option('--admin-username', type=str, expose_value=False, callback=param_callback, envvar='AE5_ADMIN_USERNAME',
                 help='The username to use for admin authentication.'),
    click.option('--admin-password', type=str, expose_value=False, callback=param_callback, envvar='AE5_ADMIN_PASSWORD',
                 help='The password to use for admin authentication.'),
    click.option('--impersonate', default=False, expose_value=False, callback=param_callback, envvar='AE5_IMPERSONATE',
                 help='If true, uses impersonation to log in as the requested user. Requires credentials for a KeyCloak administrator.')
]


def login_options(password=True):
    def apply(func):
        for option in reversed(_login_options[:2 + password]):
            func = option(func)
        return func
    return apply


def get_account(required=False, admin=False):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    hostname = obj.get('hostname')
    username = obj.get('admin_username' if admin else 'username')
    matches = config.resolve(hostname, username, admin)
    if len(matches) == 1:
        hostname, username = matches[0]
    elif required:
        tstr = ' admin' if admin else ''
        ask = obj.get('is_interactive')
        if hostname or username:
            msg = 'Multiple' if len(matches) else 'No'
            msg += f'saved{tstr} accounts match'
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
            click.UsageError(f'Must supply{tstr} username and hostname')
        hostname = click.prompt('Hostname', default=hostname, type=str)
        username = click.prompt('Username', default=username, type=str)
    return hostname, username


def cluster(reconnect=False, admin=False):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    if admin:
        label = 'acluster'
        cls = AEAdminSession
    else:
        label = 'cluster'
        cls = AEUserSession
    if label not in obj or reconnect:
        hostname, username = get_account(required=True, admin=admin)
        obj[label] = cls(hostname, username, obj.get('password'))
    return obj[label]


def cluster_call(method, *args, **kwargs):
    admin = kwargs.pop('admin', False)
    try:
        return getattr(cluster(admin=admin), method)(*args, **kwargs)
    except Exception as e:
        raise
        raise click.UsageError(str(e))
