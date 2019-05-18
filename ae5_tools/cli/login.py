import click

from ..config import config
from ..api import AESessionBase, AEUserSession, AEAdminSession, AEAuthenticationError
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
    click.option('--impersonate', is_flag=True, expose_value=False, callback=param_callback, envvar='AE5_IMPERSONATE',
                 help='If true, uses impersonation to log in as the requested user. Requires credentials for a KeyCloak administrator.')
]


def login_options(password=True):
    def apply(func):
        for option in reversed(_login_options):
            func = option(func)
        return func
    return apply


def get_account(admin=False):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    hostname = obj.get('hostname')
    key = 'admin_username' if admin else 'username'
    username = obj.get(key)
    if hostname and username:
        return hostname, username
    if not obj.get('is_interactive'):
        raise click.UsageError('Username and hostname must both be supplied in non-interactive sessions')
    matches = config.resolve(hostname, username, admin)
    if len(matches) == 1:
        hostname, username = matches[0]
    else:
        if not hostname:
            d_hostname = matches[0][0] if matches else None
            hostname = click.prompt('Hostname', default=d_hostname, type=str)
            matches = config.resolve(hostname, username, admin)
        if not username:
            d_username = matches[0][1] if matches else None
            prompt = 'Admin username' if admin else 'Username'
            username = click.prompt(prompt, default=d_username, type=str)
    obj['hostname'] = hostname
    obj[key] = username
    return hostname, username


def click_password(key):
    return click.prompt(f'Password for {key}', type=str, hide_input=True)


def cluster(reconnect=False, admin=False):
    ctx = click.get_current_context()
    obj = ctx.ensure_object(dict)
    label = 'acluster' if admin else 'cluster'
    hostname, username = get_account(admin=admin)
    impersonate = obj['impersonate']
    if not obj.get(label) or reconnect:
        AESessionBase._password_prompt = click_password
        try:
            if admin:
                conn = AEAdminSession(hostname, username, obj.get('admin_password'),
                                      password_prompt=click_password)
            else:
                try:
                    conn = AEUserSession(hostname, username, obj.get('password'),
                                         retry=not impersonate, password_prompt=click_password)
                except AEAuthenticationError as e:
                    if not impersonate:
                        raise click.ClickException(str(e))
                    click.echo(f'Impersonating {username}@{hostname}...')
                    conn = cluster(reconnect, True).impersonate(username)
            obj[label] = conn
        except ValueError as e:
            raise click.ClickException(str(e))
    return obj[label]


def cluster_call(method, *args, **kwargs):
    try:
        c = cluster(admin=kwargs.pop('admin', False))
        return getattr(c, method)(*args, **kwargs)
    except Exception as e:
        raise click.ClickException(str(e))
