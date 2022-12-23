from ....contract.dto.base_model import BaseModel


class SecretNamesGetResponse(BaseModel):
    secrets: list[str]
