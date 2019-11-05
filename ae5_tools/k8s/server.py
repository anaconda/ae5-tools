import re
import json
import asyncio

from aiohttp import web
from urllib.parse import urlencode

from .transformer import AE5K8STransformer, WebStream


DEFAULT_K8S_URL = 'https://10.100.0.1/'
DEFAULT_K8S_TOKEN_FILE = '/var/run/secrets/user_credentials/k8s_token'


def _json(result):
    text = json.dumps(result, indent=2)
    return web.Response(text=text, content_type='application/json')


class WebStream(object):
    def __init__(self, stream, request):
        self.stream = web.StreamResponse(status=200, reason='OK', headers={'Content-Type': 'text/plain'})
        await self.stream.prepare(request)
        self.request = request
    def closing(self):
        return self.request.is_closing()
    async def stream(self, data):
        return self.stream.write(data)


class AE5K8SHandler(object):
    def __init__(self, url, token):
        self.xfrm = AE5K8STransformer(url, token)

    async def cleanup(self):
        await self.xfrm.close()

    async def hello(self, request):
        return web.Response(text="Alive and kicking")

    async def nodeinfo(self, request):
        result = await self.xfrm.node_info_()
        return _json(result)

    async def _podinfo(self, ids):
        is_single = isinstance(ids, str)
        idset = set([ids] if is_single else ids)
        results = await asyncio.gather(*(self.xfrm.pod_info_(idf) for id in idset), return_exceptions=True)
        invalid = [id for id, q in zip(ids, results) if isinstance(q, Exception)]
        if invalid:
            plural = "s" if len(invalid) > 1 else ""
            raise web.HTTPUnprocessableEntity(reason=f'Invalid or missing ID{plural}: {", ".join(invalid)}')
        if is_single:
            return results[0]
        values = dict(zip(idset, results))
        return values

    async def podinfo_get_query(self, request):
        if not request.query:
            raise web.HTTPUnprocessableEntity(reason='Must supply an ID')
        invalid_keys = set(k for k in request.query if k != 'id')
        if invalid_keys:
            query = urlencode(request.query)
            raise web.HTTPUnprocessableEntity(reason=f'Invalid query: {query}')
        return _json(await self._podinfo(request.query.values()))

    async def podinfo_get_path(self, request):
        return _json(await self._podinfo(request.match_info['id']))

    async def podlog(self, request):
        id = request.match_info['id']
        if 'container' in request.query:
            container = ','.join(v for k, v in request.query.items() if k == 'container')
        else:
            container = None
        if 'follow' in request.query:
            value = ','.join(v for k, v in request.query.items() if k == 'follow')
            if value not in ('', 'true', 'false'):
                raise web.HTTPUnprocessableEntity(reason=f'Invalid parameter: follow={values[0]}')
            follow = value != 'false'
        else:
            follow = False
        try:
            await self.xfrm.pod_log(id, container, follow=follow, stream=WebStream(stream))
            await stream.write_eof()
        except KeyError as exc:
            raise web.HTTPUnprocessableEntity(reason=str(exc))
        return stream


def main(url=None, token=None, port=None):
    port = port or int(os.environ.get('AE5_K8S_PORT') or '8086')
    url = url, os.environ.get('AE5_K8S_URL', DEFAULT_K8S_URL)
    token = token or os.environ.get('AE5_K8S_TOKEN')
    if token is None:
        token_file = os.environ.get('AE5_K8S_TOKEN_FILE', DEFAULT_K8S_TOKEN_FILE)
        if token_file and os.path.exists(token_file):
            with open(token_file, 'r') as fp:
                token = fp.read().strip()
    app = web.Application()
    handler = AE5K8SHandler(url, token)
    app.add_routes([web.get('/'), handler.hello,
                    web.get('/__status__'), handler.hello,
                    web.get('/nodes'), handler.nodeinfo,
                    web.get('/pods'), handler.podinfo_get_query,
                    web.get('/pod/{id}'), handler.podinfo_get_path,
                    web.get('/pod/{id}/log', handler.podlog)])
    app.on_cleanup.append(handler.cleanup_transformer)
    web.run_app(app, port=port or 8086)


if __name__ == '__main__':
    main()
