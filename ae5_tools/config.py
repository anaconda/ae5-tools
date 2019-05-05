import stat
import getpass
import json
import os
import warnings


def yes_no(question, default=None):
    dstr = 'Y/n' if default == 'y' else 'y/N' if default == 'n' else 'Y/N'
    while True:
        response = input(f'{question} [{default}]? ')
        response = response.lower()[:1] or default
        if response in ('y', 'n'):
            return response == 'y'
        print("Must enter 'y' or 'n'.")


def my_input(request, default=None, required=True, hidden=False):
    if not default:
        request = f'{request}: '
    elif hidden:
        request = f'{request} [********]: '
    else:
        request = f'{request} [{default}]: '
    while True:
        response = (getpass.getpass if hidden else input)(request) or default
        if response or not required:
            return response
        print('Must supply a value.')


class ConfigManager:
    def __init__(self):
        fdir = os.getenv('AE5_TOOLS_CONFIG_DIR') or '~/.ae5'
        fpath = os.path.expanduser(os.path.join(fdir, 'creds'))
        self._path = fpath
        self._data = {}
        self.read()

    def read(self):
        if os.path.isfile(self._path):
            with open(self._path, 'r') as fp:
                text = fp.read()
            if text:
                try:
                    data = json.loads(text)
                except json.decoder.JSONDecodeError:
                    warnings.warn(f'Credentials file {fpath} is corrupt; no credentials loaded.')
            self._data = {tuple(k.rsplit('@', 1)): v for k, v in data.items()}

    def write(self):
        os.makedirs(os.path.dirname(self._path), mode=0o700, exist_ok=True)
        nfile = self._path + '.new'
        with open(nfile, 'w') as fp:
            data = {'@'.join(k): v for k, v in self._data.items()}
            json.dump(data, fp, sort_keys=True, indent=2)
        os.chmod(nfile, 0o600)
        os.rename(nfile, self._path)

    def new(self, hostname=None, username=None, password=None,
            interactive=True, store=None, default=None):
        if not (hostname and username and password):
            if not interactive:
                raise RuntimeError('Must supply hostname, username, and password')
            hostname = my_input('Hostname', hostname)
            username = my_input('Username', username)
            password = my_input('Password', password, hidden=True)
        if store is None and interactive:
            store = yes_no('Save for future use', 'y')
        label = (username, hostname)
        self._data[label] = password
        if default is None:
            if not store:
                default = True
            elif interactive:
                default = yes_no('Make default', 'y')
        if default and len(self._data) > 1:
            ndata = {label: self._data[label]}
            ndata.update((k, v) for k, v in self._data.items() if k != label)
            self._data = ndata
        if store:
            self.write()
        return hostname, username, password

    def count(self):
        return len(self._data)

    def list(self):
        print('\n'.join('@'.join(k) for k in self._data))

    def default(self):
        if not self._data:
            raise RuntimeError('No saved credentials')
        (u, h), p = next(v for v in self._data.items())
        return h, u, p

    def find(self, hostname=None, username=None, interactive=False):
        if not self._data:
            if interactive:
                return config.new(interactive=True)
            raise RuntimeError('No saved credentials')
        if hostname is None and username is None:
            return self.default()
        matches = [(u, h, p) for (u, h), p in self._data.items()
                   if (hostname is None or hostname == h)
                   and (username is None or username == u)]
        if len(matches) == 1:
            return matches[0]
        if interactive and not matches:
            return config.new(interactive=True)
        if interactive:
            u0, h0, p0 = matches[0]
            return config.new(username=u0 if all(u0 == u for u, _, _ in matches) else None,
                              hostname=h0 if all(h0 == h for _, h, _ in matches) else None,
                              password=p0 if all(p0 == p for _, _, p in matches) else None,
                              interactive=True)
        msg = 'Multiple ' if matches else 'No '
        msg += 'credentials with'
        if hostname:
            msg += f' hostname "{hostname}"'
        if username and hostname:
            msg += ' and'
        if username:
            msg += f' username "{username}"'
        msg += ' were found'
        if matches:
            matches = [f'  - {u}@{h}' for u, h, _ in matches]
            msg += '\n'.join([':'] + matches)
        raise ValueError(msg)

config = ConfigManager()

