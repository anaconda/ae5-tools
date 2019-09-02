import click

from ..config import config
from ..api import AESessionBase, AEUserSession, AEAdminSession, AEException
from .utils import param_callback, click_text, get_options, persist_option
from .format import print_output
from ..identifier import Identifier


def print_login_help(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click_text('''
@Logging into the AE5 cluster
----------------------------

The CLI provides a number of options that can be used with most commands
to control authentication to the cluster. They can be supplied on the command
line or by setting environment variables given in parentheses below.

Login sessions are saved to disk unless --no-saved-logins
option is supplied. This enables multiple AE5 calls to be made
without re-entering a password. Passwords are *not* saved to disk, just
the session and authentication cookies generated from it. The cookie
file is saved with secure permissions, and is subject to expiration
timeouts identical to interactive web sessions.

If the hostname option is not supplied, then the hostname of the last saved
session is used, even if its cookies are expired. If no saved session exists,
the user will be prompted for a hostname. Similarly, default values of the
username or admin username will be determined from the last saved sessions
on the given hostname.

@Options:
''')
    for option, help in _login_help.items():
        text = f'--{option}'
        spacer = ' ' * (20 - len(text))
        text = f'{text}{spacer}{help}'
        click.echo(click.wrap_text(text, initial_indent='  ', subsequent_indent=' ' * 22))
    ctx.exit()


_login_help = {
    'hostname': 'Hostname of the cluster. (AE5_HOSTNAME)',
    'username': 'Username for standard access. (AE5_USERNAME)',
    'password': 'Password for standard access. (AE5_PASSWORD)',
    'admin-username': 'Username for Keycloak admin access. (AE5_ADMIN_USERNAME)',
    'admin-password': 'Password for Keycloak admin access. (AE5_ADMIN_PASSWORD)',
    'impersonate': ('Use the Keycloak administrator account to impersonate the '
                    'given user instead of requiring the user password. '
                    '(AE5_IMPERSONATE)'),
    'no-saved-logins': ('Do not load or save login sessions to/from disk. A password '
                        'must be supplied, and its session will persist only for the '
                        'duration of AE5 call, including multiple commands in REPL mode. '
                        '(AE5_NO_SAVED_LOGINS)')
}


_login_options = [
    click.option('--hostname', type=str, default=None, expose_value=False, callback=param_callback, envvar='AE5_HOSTNAME', hidden=True),
    click.option('--username', type=str, default=None, expose_value=False, callback=param_callback, envvar='AE5_USERNAME', hidden=True),
    click.option('--password', type=str, default=None, expose_value=False, callback=param_callback, envvar='AE5_PASSWORD', hidden=True),
    click.option('--admin-username', type=str, default=None, expose_value=False, callback=param_callback, envvar='AE5_ADMIN_USERNAME', hidden=True),
    click.option('--admin-password', type=str, default=None, expose_value=False, callback=param_callback, envvar='AE5_ADMIN_PASSWORD', hidden=True),
    click.option('--impersonate', is_flag=True, default=None, expose_value=False, callback=param_callback, envvar='AE5_IMPERSONATE', hidden=True),
    click.option('--no-saved-logins', is_flag=True, default=None, expose_value=False, callback=param_callback, envvar='AE5_NO_SAVED_LOGINS', hidden=True),
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
    opts = get_options()
    hostname = opts.get('hostname')
    key = 'admin_username' if admin else 'username'
    username = opts.get(key)
    if hostname and username:
        return hostname, username
    matches = config.resolve(hostname, username, admin)
    if len(matches) >= 1:
        hostname, username = matches[0]
    else:
        if not hostname:
            hostname = click.prompt('Hostname', type=str, err=True)
            matches = config.resolve(hostname, username, admin)
        if not username:
            prompt = 'Admin username' if admin else 'Username'
            username = click.prompt(prompt, type=str, err=True)
    persist_option('hostname', hostname)
    persist_option(key, username)
    return hostname, username


def _click_auth_message(msg, nl=True):
    click.echo(msg, err=True, nl=nl)


SESSIONS = {}


def cluster_connect(hostname, username, admin):
    opts = get_options()
    key = (hostname, username, admin)
    conn = SESSIONS.get(key)
    if conn is None:
        atype = 'admin' if admin else 'user'
        click.echo(f'Connecting to {atype} account {username}@{hostname}.', err=True)
        AESessionBase._auth_message = _click_auth_message
        try:
            session_save = not opts.get('no_saved_logins', False)
            if admin:
                conn = AEAdminSession(hostname, username, opts.get('admin_password'),
                                      persist=session_save)
            else:
                if opts.get('impersonate'):
                    password = cluster(True)
                else:
                    password = opts.get('password')
                conn = AEUserSession(hostname, username, password,
                                     persist=session_save)
            SESSIONS[key] = conn
        except (ValueError, AEException) as e:
            raise click.ClickException(str(e))
    return SESSIONS.get(key)


def cluster_disconnect(admin=False):
    hostname, username = get_account(admin=admin)
    conn = cluster_connect(hostname, username, admin)
    if conn is not None and conn.connected:
        conn.disconnect()
        click.echo(f'Logged out as {username}@{hostname}.', err=True)
        del SESSIONS[(hostname, username, admin)]


def cluster(admin=False, retry=True):
    hostname, username = get_account(admin=admin)
    return cluster_connect(hostname, username, admin)


def cluster_call(method, *args, **kwargs):
    # Translate the user-supplied identifer string into an ID
    ident = kwargs.pop('ident', None)
    if ident:
        id_class = kwargs.pop('id_class', None)
        revision = id_class == 'project'
        if not id_class:
            id_class = method.split('_', 1)[0]
        result = cluster_call(id_class + '_info', ident, internal=True, format='json')
        ident = Identifier.from_record(result, ignore_revision=not revision)
        args = (result['id'],) + args

    # Provide a standardized method for providing interactive output
    # on the cli, including a confirmation prompt, a simple progress
    # indicator via prefix/postfix strings
    is_cli = (kwargs.pop('cli', False) or 'confirm' in kwargs
              or 'prefix' in kwargs or 'postfix' in kwargs)
    if is_cli:
        confirm = kwargs.pop('confirm', None) or ''
        prefix = kwargs.pop('prefix', None) or ''
        postfix = kwargs.pop('postfix', None) or ''
        if ident:
            confirm = confirm.format(ident=ident)
            prefix = prefix.format(ident=ident)
            postfix = postfix.format(ident=ident)

        if confirm and not click.confirm(confirm):
            return
        if prefix:
            click.echo(prefix, nl=False, err=True)
        format = get_options().get('format') or 'table'
        kwargs.setdefault('format', 'table' if format in ('json', 'csv') else 'table')

    # Retrieve the proper cluster session object and make the call
    try:
        admin = kwargs.pop('admin', False)
        c = cluster(admin=admin)
        result = getattr(c, method)(*args, **kwargs)
    except AEException as e:
        raise click.ClickException(str(e))

    # Finish out the standardized CLI output
    if is_cli:
        if postfix or prefix:
            click.echo(postfix, nl=True, err=True)
        print_output(result)

    return result
