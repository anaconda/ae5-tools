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
        self.cookies = []
        self.tokens = []
        for label in ('cookies', 'tokens'):
            data = getattr(self, label)
            cpath = os.path.join(self._path, label)
            if os.path.isdir(cpath):
                data = [fname for fname in os.listdir(cpath)
                        if not fname.startswith('.')
                        and len(fname.split('@')) == 2]
                setattr(self, label,
                        sorted(data, key=lambda x: os.path.getmtime(os.path.join(cpath, x)), reverse=True))

    def list(self):
        from_zone = tz.tzutc()
        to_zone = tz.tzlocal()
        result = []
        for key in self.cookies:
            username, hostname = key.rsplit('@', 1)
            is_admin = False
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
            result.append((hostname, username, is_admin, status))
        for key in self.tokens:
            username, hostname = key.rsplit('@', 1)
            is_admin = True
            fname = os.path.join(self._path, 'tokens', key)
            if os.path.isfile(fname):
                with open(fname, 'r') as fp:
                    sdata = fp.read()
                sdata = json.loads(sdata) if sdata else {}
                if 'refresh_expires_in' in sdata:
                    expires = os.path.getmtime(fname) + int(sdata['refresh_expires_in'])
                    expires = datetime.fromtimestamp(expires)
                    if expires < datetime.now():
                        status = 'expired'
                    else:
                        status = expires.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    status = 'unknown'
            else:
                status = 'no session'
            result.append((hostname, username, is_admin, status))
        return result

    def resolve(self, hostname=None, username=None, admin=False):
        matches = []
        data = self.tokens if admin else self.cookies
        for key in data:
            u, h = key.rsplit('@', 1)
            if (hostname is None or hostname == h) and (username is None or username == u):
                matches.append((h, u))
        return matches

    def admin_session(self, hostname, username, password=None):
        login_url = f'https://{hostname}/auth/realms/master/protocol/openid-connect/token'
        if not hostname or not username:
            raise ValueError('Must supply username and hostname')
        key = f'{username}@{hostname}'
        fname = os.path.join(self._path, 'tokens', key)
        session = requests.Session()
        session.verify = False
        sdata = None
        if os.path.exists(fname):
            with open(fname, 'r') as fp:
                sdata = fp.read()
            if sdata:
                sdata = json.loads(sdata)
        if sdata and 'refresh_token' in sdata:
            r = session.post(login_url, data={'refresh_token': sdata['refresh_token'],
                                              'grant_type': 'refresh_token',
                                              'client_id': 'admin-cli'})
            if r.status_code == 200:
                token = r.json()["access_token"]
                session.headers['Authorization'] = f'Bearer {token}'
                return session
            print(f'Admin token for {username}@{hostname} has expired; must log in again.')
        else:
            print('Admin login required for {username}@{hostname}.')
        while not password:
            password = getpass.getpass(f'Password: ')
            if not password:
                print('Must supply a password.')
        r = session.post(login_url, data={'username': username, 'password': password,
                                          'grant_type': 'password',
                                          'client_id': 'admin-cli'})
        if r.status_code != 200:
            raise RuntimeError('Falied to create admin session')
        sdata = r.json()
        session.headers['Authorization'] = f'Bearer {sdata["access_token"]}'
        os.makedirs(os.path.dirname(fname), mode=0o700, exist_ok=True)
        with open(fname, 'w') as fp:
            json.dump(sdata, fp)
        os.chmod(fname, 0o600)
        return session

    def session(self, hostname, username, password=None):
        if not hostname or not username:
            raise ValueError('Must supply username and hostname')
        key = f'{username}@{hostname}'
        fname = os.path.join(self._path, 'cookies', key)
        cookies = LWPCookieJar(fname)
        session = requests.Session()
        session.cookies = cookies
        session.verify = False
        if os.path.exists(fname):
            cookies.load()
            r = session.get(f'https://{hostname}/api/v2/runs')
            if r.status_code == 200:
                for cookie in cookies:
                    if cookie.name == '_xsrf':
                        session.headers['x-xsrftoken'] = cookie.value
                        break
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
                    break
            os.makedirs(os.path.dirname(fname), mode=0o700, exist_ok=True)
            cookies.save()
            os.chmod(fname, 0o600)
            return session
        raise RuntimeError('Falied to create login session')


config = ConfigManager()

