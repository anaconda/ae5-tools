import asyncio
from typing import Any, Optional

import aiohttp

from ...contracts.dto.base_model import BaseModel


class AEBaseTransformer(BaseModel):
    url: Optional[str] = None
    token: Optional[str] = None
    headers: dict = {"accept": "application/json"}
    session: Optional[Any] = None
    has_metrics: Optional[Any] = None

    def __init__(self, **data: Any):
        super().__init__(**data)

        if self.token:
            self.headers["authorization"] = f"Bearer {self.token}"
        self.url = self.url.rstrip("/")

    async def connect(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))

    async def close(self):
        if self.session is not None:
            await self.session.close()
            self.session = None

    def __del__(self):
        if self.session is not None:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self.close())

    async def get(self, path, type="json", ok404=False):
        await self.connect()
        if not path.startswith("/"):
            path = "/api/v1/" + path
        url = self.url + path
        async with self.session.get(url, headers=self.headers) as resp:
            if resp.status == 404 and ok404:
                return
            resp.raise_for_status()
            if type == "json":
                return await resp.json()
            elif type == "text":
                return await resp.text()
            else:
                return resp
