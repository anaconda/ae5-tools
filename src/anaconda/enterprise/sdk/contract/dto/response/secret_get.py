from ....contract.dto.base_model import BaseModel


class SecretGetResponse(BaseModel):
    secrets: list[str]
