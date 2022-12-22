from ..base_model import BaseModel


class SecretPutRequest(BaseModel):
    key: str
    value: str
