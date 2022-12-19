from aiohttp import web


class WebStream(object):
    def __init__(self, request):
        self._request = request

    async def prepare(self, request):
        self._response = web.StreamResponse(headers={"Content-Type": "text/plain"})
        await self._response.prepare(self._request)

    def closing(self):
        return self._request.protocol.transport.is_closing()

    async def write(self, data):
        await self._response.write(data)

    async def finish(self):
        return await self._response.write_eof()
