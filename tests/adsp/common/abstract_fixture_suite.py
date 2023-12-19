from __future__ import annotations

from abc import abstractmethod

from pydantic import BaseModel, ConfigDict

from tests.adsp.common.fixture_manager import FixtureManager


class AbstractFixtureSuite(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    config: dict
    manager: FixtureManager

    @abstractmethod
    def _setup(self) -> None:
        """ """

    def __enter__(self):
        self._setup()

    def __exit__(self, type, value, traceback):
        del self.manager
