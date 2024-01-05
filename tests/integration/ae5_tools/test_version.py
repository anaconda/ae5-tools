from click.testing import CliRunner

from ae5_tools._version import get_versions
from ae5_tools.cli.main import cli


def test_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert f"{get_versions().get('version')}" in result.output

    # This won't work because it thinks cli is the app_nane
    # assert "ae5 " in result.output
