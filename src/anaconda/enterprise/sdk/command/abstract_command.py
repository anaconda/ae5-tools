from abc import abstractmethod
from typing import Any, Optional

from ..contract.dto.base_model import BaseModel


class AbstractCommand(BaseModel):
    @abstractmethod
    def execute(self, *args, **kwargs) -> Optional[Any]:
        """Command entry point"""
