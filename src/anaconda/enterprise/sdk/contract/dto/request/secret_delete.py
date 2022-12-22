from ..base_model import BaseModel


class SecretDeleteRequest(BaseModel):
    key: str
