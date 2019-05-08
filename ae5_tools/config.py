import getpass
import json
import os
import sys
import requests
import urllib3

from http.cookiejar import LWPCookieJar
from datetime import datetime
from dateutil import tz

from lxml import html

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ConfigManager:
    def __init__(self):
        self._path = os.path.expanduser(os.getenv('AE5_TOOLS_CONFIG_DIR') or '~/.ae5')
        self._data = {}
        self.read()

    def read(self):
        fpath = os.path.join(self._path, 'config')
        data = {}
        if os.path.isfile(fpath):
            with open(fpath, 'r') as fp:
                text = fp.read()
            if text:
                try:
                    data = json.loads(text)
                except json.decoder.JSONDecodeError:
                    raise RuntimeError(f'Configuration file {fpath} is corrupt; please remove or correct')
        data.setdefault('default', None)
        accounts = data.setdefault('accounts', {})
        cpath = os.path.join(self._path, 'cookies')
        if os.path.isdir(cpath):
            for fname in os.listdir(cpath):
                if len(fname.split('@')) == 2 and os.path.isfile(os.path.join(cpath, fname)):
                    accounts.setdefault(fname, None)
        self._data = data

    def write(self, must_succeed=True):
        try:
            fpath = os.path.join(self._path, 'config')
            os.makedirs(self._path, mode=0o700, exist_ok=True)
            nfile = fpath + '.new'
            with open(nfile, 'w') as fp:
                json.dump(self._data, fp, sort_keys=False, indent=2)
            os.chmod(nfile, 0o600)
            os.rename(nfile, fpath)
        except Exception:
            if must_succeed:
                raise

    def set_default(self, default):
        odefault = self._data.get('default')
        if default != odefault:
            self._data['default'] = default
            self.write()

    def store(self, hostname, username, password, default=False):
        dirty = False
        key = f'{username}@{hostname}'
        opassword = self._data['accounts'].get(key)
        if opassword != password:
            if password:
                self._data['accounts'][key] = password
            else:
                del self._data['accounts'][key]
            dirty = True
        odefault = self._data.get('default')
        if default != odefault:
            self._data['default'] = default
            dirty = True
        if dirty:
            self.write()

    def count(self):
        return len(self._data['accounts'])

    def list(self):
        from_zone = tz.tzutc()
        to_zone = tz.tzlocal()
        result = []
        for key, value in self._data['accounts'].items():
            username, hostname = key.rsplit('@', 1)
            fname = os.path.join(self._path, 'cookies', key)
            if os.path.isfile(fname):
                cookies = LWPCookieJar(fname)
                cookies.load()
                expires = min(cookie.expires for cookie in cookies)
                expired = any(cookie.is_expired() for cookie in cookies)
            else:
                status = 'no session'
            if expired:
                status = 'expired'
            else:
                expires = (datetime.utcfromtimestamp(expires)
                           .replace(tzinfo=from_zone).astimezone(to_zone))
                status = expires.strftime('%Y-%m-%d %H:%M:%S')
            result.append((hostname, username, bool(value), status))
        return result

    def default(self):
        key = self._data['default']
        if key:
            u, h, = key.rsplit('@', 1)
            return h, u

    def resolve(self, hostname=None, username=None):
        matches = []
        for key in self._data['accounts']:
            u, h = key.rsplit('@', 1)
            if (hostname is None or hostname == h) and (username is None or username == u):
                matches.append((h, u))
            if default:
                break
        return matches

    def session(self, hostname, username, password=None):
        if not hostname or not username:
            raise ValueError('Must supply username and hostname')
        key = f'{username}@{hostname}'
        if password is None:
            password = self._data['accounts'].get(key)
        fname = os.path.join(self._path, 'cookies', key)
        cookies = LWPCookieJar(fname)
        session = requests.Session()
        session.cookies = cookies
        session.verify = False
        if os.path.exists(fname):
            cookies.load()
            for cookie in cookies:
                if cookie.name == '_xsrf' and not cookie.is_expired():
                    session.headers['x-xsrftoken'] = cookie.value
                    r = session.get(f'https://{hostname}/api/v2/runs')
                    if r.status_code == 200:
                        return session
            print(f'Session for {username}@{hostname} has expired; must log in again.')
        else:
            print(f'Logging into {username}@{hostname}...')
        while not password:
            password = getpass.getpass(f'Password: ')
            if not password:
                print('Must supply a password.')
        url = f'https://{hostname}/auth/realms/AnacondaPlatform/protocol/openid-connect/auth?client_id=anaconda-platform&scope=openid&response_type=code&redirect_uri=https%3A%2F%2F{hostname}%2Flogin'
        r = session.get(url)
        tree = html.fromstring(r.text)
        form = tree.xpath("//form[@id='kc-form-login']")
        login_url = form[0].action
        r = session.post(login_url, data={'username': username, 'password': password})
        if r.status_code == 200:
            for cookie in cookies:
                if cookie.name == '_xsrf':
                    session.headers['x-xsrftoken'] = cookie.value
                    os.makedirs(os.path.dirname(fname), mode=0o700, exist_ok=True)
                    cookies.save()
                    os.chmod(fname, 0o600)
                    if self._data['default'] is None:
                        self.set_default(key)
                    return session
        raise RuntimeError('Unable to retrieve token')


config = ConfigManager()

