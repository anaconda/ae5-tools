import os
import unittest

from ae5_tools import EnvironmentVariableNotFoundError, demand_env_var, demand_env_var_as_bool, get_env_var


class TestEnvironment(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["FAKE_VALUE"] = "mock-value"
        os.environ["INT_VALUE"] = "1"
        os.environ["FLOAT_VALUE"] = "3.14"

    def tearDown(self) -> None:
        if "FAKE_VALUE" in os.environ:
            del os.environ["FAKE_VALUE"]
        if "INT_VALUE" in os.environ:
            del os.environ["INT_VALUE"]
        if "FLOAT_VALUE" in os.environ:
            del os.environ["FLOAT_VALUE"]

    def test_demand_env_var(self):
        self.assertEqual(demand_env_var("FAKE_VALUE"), os.environ["FAKE_VALUE"])

    def test_demand_env_var_should_gracefully_fail(self):
        with self.assertRaises(EnvironmentVariableNotFoundError) as context:
            demand_env_var("SOME_VALUE")
        self.assertEqual(str(context.exception), "Environment variable (SOME_VALUE) not found")

    def test_get_env_var(self):
        self.assertEqual(get_env_var("FAKE_VALUE"), os.environ["FAKE_VALUE"])

    def test_get_env_var_should_gracefully_fail(self):
        self.assertIsNone(get_env_var("SOME_VALUE"))

    def test_demand_env_var_as_bool(self):
        os.environ["FAKE_VALUE"] = "false"
        self.assertEqual(demand_env_var_as_bool("FAKE_VALUE"), False)
        os.environ["FAKE_VALUE"] = "0"
        self.assertEqual(demand_env_var_as_bool("FAKE_VALUE"), False)
        os.environ["FAKE_VALUE"] = "true"
        self.assertEqual(demand_env_var_as_bool("FAKE_VALUE"), True)
        os.environ["FAKE_VALUE"] = "1"
        self.assertEqual(demand_env_var_as_bool("FAKE_VALUE"), True)

        os.environ["FAKE_VALUE"] = "FAKE_VALUE"
        with self.assertRaises(EnvironmentVariableNotFoundError) as context:
            demand_env_var_as_bool("FAKE_VALUE")
        self.assertEqual(str(context.exception), "Environment variable (FAKE_VALUE) not boolean and can not be loaded")


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(TestEnvironment())
