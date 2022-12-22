from ....contract.dto.base_model import BaseModel


class SecretsGetResponse(BaseModel):
    secrets: list[str]
