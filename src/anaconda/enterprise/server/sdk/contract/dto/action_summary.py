import datetime
from typing import Union

from anaconda.enterprise.server.contracts import BaseModel

from .types.action_summary_status import AEActionSummaryStatusType


class AEActionSummary(BaseModel):
    message: str
    updated: datetime.datetime
    id: str
    owner: str
    status: Union[AEActionSummaryStatusType, str]  # TODO: the enumeration needs further definition.
    created: datetime.datetime
    type: Union[AEActionSummaryStatusType, str]  # TODO: the enumeration needs further definition.
    done: bool
    error: bool
