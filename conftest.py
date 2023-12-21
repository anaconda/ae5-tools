import pytest

from ae5_tools.api import AEAdminSession, AEUserSession
from tests.adsp.common.utils import _get_vars
from tests.system.state import load_account


def pytest_addoption(parser):
    parser.addoption("--ci-skip", action="store_true", default=False, help="Disable Tests Broken In CI")


def pytest_configure(config):
    config.addinivalue_line("markers", "ci-skip: mark test as skipped in ci")


def pytest_collection_modifyitems(config, items):
    #  Skip config
    if config.getoption("--ci-skip"):
        ci_skip_marker = pytest.mark.skip(reason="Failing against CI due to environmental issues")
        for item in items:
            if "ci_skip" in item.keywords:
                item.add_marker(ci_skip_marker)
