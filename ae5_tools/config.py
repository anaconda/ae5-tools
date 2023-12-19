import json
import logging
import os
from datetime import datetime
from http.cookiejar import LWPCookieJar

from dateutil import tz

from .exceptions import AE5ConfigError

RC_DIR = "~/.ae5"
CONFIG_FILE = "config.py"
logger = logging.getLogger(__name__)


class ConfigManager:
    def __init__(self):
        self._path = os.path.expanduser(os.getenv("AE5_TOOLS_CONFIG_DIR") or RC_DIR)
        self.init_path()
        self._data = {}
        self.load()

    def init_path(self):
        """Ensure the config directory exists"""
        logger.debug(f"Creating directory: {self._path}")
        try:
            os.makedirs(self._path, mode=0o700, exist_ok=True)
        except OSError as e:
            # todo:  log e.errno - 13 means bad permissions
            #        17: file exists
            msg = f"Cannot create directory {self._path}. Please check directory permissions."
            raise AE5ConfigError(msg)

    @property
    def config_path(self):
        return os.path.join(self._path, CONFIG_FILE)

    def load(self):
        self._data = {}
        cpath = self.config_path
        if os.path.isfile(cpath):
            with open(cpath, "r") as fp:
                data = fp.read()
            if data.startswith("{"):
                self._data.update(json.loads(data))
        for label in ("cookies", "tokens"):
            cpath = os.path.join(self._path, label)
            if os.path.isdir(cpath):
                files = [os.path.join(cpath, fname) for fname in os.listdir(cpath) if not fname.startswith(".") and len(fname.split("@")) == 2]
                files = sorted(files, key=lambda x: os.path.getmtime(x), reverse=True)
            else:
                files = []
            setattr(self, label, files)

    def save(self):
        self.init_path()
        with open(self.config_path, "w") as fp:
            json.dump(self._data, fp)

    def list(self):
        from_zone = tz.tzutc()
        to_zone = tz.tzlocal()
        result = []
        for fname in self.cookies:
            key = os.path.basename(fname)
            username, hostname = key.rsplit("@", 1)
            last = datetime.fromtimestamp(os.path.getmtime(fname))
            last = last.strftime("%Y-%m-%d %H:%M:%S")
            is_admin = False
            cookies = LWPCookieJar(fname)
            cookies.load()
            expires = min(cookie.expires for cookie in cookies)
            expired = any(cookie.is_expired() for cookie in cookies)
            if expired:
                status = "expired"
            else:
                expires = datetime.utcfromtimestamp(expires).replace(tzinfo=from_zone).astimezone(to_zone)
                status = expires.strftime("%Y-%m-%d %H:%M:%S")
            result.append((hostname, username, is_admin, last, status))
        for fname in self.tokens:
            key = os.path.basename(fname)
            username, hostname = key.rsplit("@", 1)
            is_admin = True
            if os.path.isfile(fname):
                last = datetime.fromtimestamp(os.path.getmtime(fname))
                last = last.strftime("%Y-%m-%d %H:%M:%S")
                with open(fname, "r") as fp:
                    sdata = fp.read()
                sdata = json.loads(sdata) if sdata else {}
                if "refresh_expires_in" in sdata:
                    expires = os.path.getmtime(fname) + int(sdata["refresh_expires_in"])
                    expires = datetime.fromtimestamp(expires)
                    if expires < datetime.now():
                        status = "expired"
                    else:
                        status = expires.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    status = "unknown"
            else:
                status = "no session"
            result.append((hostname, username, is_admin, last, status))
        return result

    def resolve(self, hostname=None, username=None, admin=False):
        if hostname and username:
            return [(hostname, username)]
        matches = []
        data = self.tokens if admin else self.cookies
        if not hostname or not username:
            for fname in data:
                key = os.path.basename(fname)
                u, h = key.rsplit("@", 1)
                if (not hostname or hostname == h) and (not username or username == u):
                    matches.append((h, u))
        return matches


config = ConfigManager()
