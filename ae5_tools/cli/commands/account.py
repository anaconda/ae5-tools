import click
import pandas as pd

from ..utils import add_param
from ..login import login_options, cluster
from ..format import print_output, format_options
from ...config import config


@click.group()
@click.pass_context
def account(ctx):
    pass


@account.command()
@format_options()
def list():
    h, u, a, t = [], [], [], []
    for hh, uu, aa, tt in config.list():
        h.append(hh)
        u.append(uu)
        a.append("Yes" if aa else "No")
        t.append(tt)
    result = pd.DataFrame({'hostname': h, 'username': u, 'admin': a, 'session expires': t})
    print_output(result)

