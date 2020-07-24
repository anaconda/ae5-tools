import os
import sys
import importlib


import pytest


from ae5_tools import exceptions

@pytest.fixture
def random_home_dir(tmpdir, monkeypatch):
    """Create a random home directory for testing ~/.ae5"""
    monkeypatch.setenv("HOME", str(tmpdir))
    return tmpdir


@pytest.fixture
def bad_home_dir(random_home_dir):
    """Create a home directory with bad permissions"""
    os.chmod(random_home_dir, 0o000)
    yield random_home_dir
    os.chmod(random_home_dir, 0o777)


@pytest.fixture
def clear_config_import():
    """
    Need to force reload config module after setting $HOME to test ~/.ae5 directory creation
    """
    def clear_import():
        try:
            del sys.modules['config']
            print("Cleared config import")
        except KeyError:
            pass

    clear_import()
    yield

    # Reset it back to unloaded
    clear_import()


def test_ae5_dir_creation(random_home_dir, clear_config_import):
    """Assume ~/.ae5 already was created, make a fake homedir and test dir creation"""
    print(f'home directory: {random_home_dir}')
    from ae5_tools import config
    # I'm not sure why i need to force-reload this after clearing it
    importlib.reload(config)

    config_dir = os.path.expanduser(config.RC_DIR)

    assert type(config.config) is config.ConfigManager
    assert os.path.exists(config_dir)
    assert config_dir == os.path.join(config.config._path)


@pytest.mark.skipif(sys.platform.startswith('win'), reason="relies on Unix permissions")
def test_ae5_dir_bad_perms(bad_home_dir, clear_config_import):
    """Bad permissions on home directory"""
    print(f'bad home directory: {bad_home_dir}')
    os.chmod(bad_home_dir, 0o000)
    with pytest.raises(exceptions.AE5ConfigError):
        from ae5_tools import config

        # I'm not sure why i need to force-reload this after clearing it
        importlib.reload(config)


def test_ae5_dir_file_exists(random_home_dir, clear_config_import):
    """A file named ~/.ae5 is blocking directory creation"""
    print(f'home directory: {random_home_dir}')

    # hard coded here because config is not loaded yet
    config_dir = os.path.join(random_home_dir, ".ae5")
    with open(config_dir, 'w'):
        pass

    with pytest.raises(exceptions.AE5ConfigError):
        from ae5_tools import config
        importlib.reload(config)


def test_ae5_dir_env_variable(clear_config_import, tmpdir, monkeypatch):
    """Test env variable: AE5_TOOLS_CONFIG_DIR"""
    monkeypatch.setenv("AE5_TOOLS_CONFIG_DIR", str(tmpdir))

    from ae5_tools import config
    importlib.reload(config)

    config_dir = tmpdir
    assert config_dir == config.config._path
    assert os.path.exists(config_dir)


#
# todo:  need to cover:]
#    - load
#    - save
#    - list
#    - resolve
#