import re

import urllib3

from .abstract import AbstractAESession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AEUserSession(AbstractAESession):
    def _set_header(self):
        s = self.session
        for cookie in s.cookies:
            if cookie.name == "_xsrf":
                s.headers["x-xsrftoken"] = cookie.value
                break

    def _connected(self):
        return any(c.name == "_xsrf" for c in self.session.cookies)

    def _is_login(self, resp):
        if resp.status_code == 200:
            ctype = resp.headers["content-type"]
            if ctype.startswith("text/html"):
                return bool(re.search(r'<form id="kc-form-login"', resp.text, re.M))

    def _disconnect(self):
        # This will actually close out the session, so even if the cookie had
        # been captured for use elsewhere, it would no longer be useful.
        self._get("/logout")
