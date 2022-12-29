from enum import Enum


class AEActionSummaryType(str, Enum):
    CREATE_ACTION = "create_action"
    DEPLOY_ACTION = "deploy_action"
