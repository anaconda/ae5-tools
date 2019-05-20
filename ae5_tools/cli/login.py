import click

from ..config import config
from ..api import AESessionBase, AEUserSession, AEAdminSession, AEAuthenticationError
from .utils import param_callback


def print_login_help(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('Logging into the AE5 cluster: options\n')
    click.echo(click.wrap_text((
'The CLI provides a number of options that can be used with most commands '
'to control the authenticatio to the cluster. These options can be supplied '
'using standard command-line options, or by setting environment variables '
'whose names are given in parentheses below.'), initial_indent='  ', subsequent_indent='  '))
    click.echo('')
    click.echo(click.wrap_text((
'For convenience, the CLI tool will re-use the last hostname and username '
'provided, unless overridden by these options. It will not request a password '
'if the previous session has not yet expired.'), initial_indent='  ', subsequent_indent='  '))
    click.echo('\nOptions:')
    for option, help in _login_help.items():
        text = f'--{option}'
        spacer = ' ' * (18 - len(text))
        text = f'{text}{spacer}{help}'
        click.echo(click.wrap_text(text, initial_indent='  ', subsequent_indent=' ' * 20))
    ctx.exit()


_login_help = {
    'hostname': 'Hostname of the cluster. (AE5_HOSTNAME)',
    'username': 'Username for user-level authentication. (AE5_USERNAME)',
    'password': 'Password for user-level authentication. (AE5_PASSWORD)',
    'admin-username': 'Keycloak admin username. (AE5_ADMIN_USERNAME)',
    'admin-password': 'Keycloak admin password. (AE5_ADMIN_PASSWORD)',
    'impersonate': ('If selected, uses impersonation to log in as the given user. '
                    'This relies on the Keycloack admin credentials instead of '
                    'requiring a user password. By default, standard user '
                    'authentication is employed. (AE5_IMPERSONATE)')
}


_login_options = [
    click.option('--hostname', type=str, expose_value=False, callback=param_callback, envvar='AE5_HOSTNAME', hidden=True),
    click.option('--username', type=str, expose_value=False, callback=param_callback, envvar='AE5_USERNAME', hidden=True),
    click.option('--password', type=str, expose_value=False, callback=param_callback, envvar='AE5_PASSWORD', hidden=True),
    click.option('--admin-username', type=str, expose_value=False, callback=param_callback, envvar='AE5_ADMIN_USERNAME', hidden=True),
    click.option('--admin-password', type=str, expose_value=False, callback=param_callback, envvar='AE5_ADMIN_PASSWORD', hidden=True),
    click.option('--impersonate', is_flag=True, expose_value=False, callback=param_callback, envvar='AE5_IMPERSONATE', hidden=True),
    click.option('--help-login', is_flag=True, callback=print_login_help, expose_value=False, is_eager=True,
                 help='Get help on the global authentication options.')
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
    matches = config.resolve(hostname, username, admin)
    if len(matches) >= 1:
        hostname, username = matches[0]
    else:
        if not hostname:
            hostname = click.prompt('Hostname', type=str)
            matches = config.resolve(hostname, username, admin)
        if not username:
            prompt = 'Admin username' if admin else 'Username'
            username = click.prompt(prompt, type=str)
    obj['hostname'] = hostname
    obj[key] = username
    if obj.get('is_console'):
        click.echo(f'Connecting as {username}@{hostname}...')
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
