import os
import unittest

from ae5_tools import load_ae5_user_secrets


class TestSecrets(unittest.TestCase):
    def test_base(self):
        secret_name: str = "MOCK_SECRET"

        if secret_name in os.environ:
            del os.environ[secret_name]

        mock_secrets_path: str = "tests/fixtures/secrets"

        with open(file=f"{mock_secrets_path}/{secret_name}", mode="r", encoding="utf-8") as file:
            expected_secret_value: str = file.read()

        load_ae5_user_secrets(secrets_path=mock_secrets_path, silent=False)
        load_ae5_user_secrets(secrets_path=mock_secrets_path, silent=True)

        self.assertEqual(os.environ[secret_name], expected_secret_value)

    def test_path_does_not_exist(self):
        secret_name: str = "MOCK_SECRET"

        if secret_name in os.environ:
            del os.environ[secret_name]

        load_ae5_user_secrets(secrets_path="some/path", silent=False)
        load_ae5_user_secrets(secrets_path="some/path", silent=True)

        self.assertFalse(secret_name in os.environ)


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(TestSecrets())
