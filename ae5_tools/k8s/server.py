import json
import os
import sys

import requests

from aiohttp import web
from urllib.parse import urlencode

from .transformer import AE5K8STransformer, AE5PromQLTransformer
from .ssh import tunneled_k8s_url


DEFAULT_K8S_URL = 'https://10.100.0.1/'
DEFAULT_K8S_TOKEN_FILE = '/var/run/secrets/user_credentials/k8s_token'
DEFAULT_PROMETHEUS_PORT = 9090


def _json(result):
    text = json.dumps(result, indent=2)
    return web.Response(text=text, content_type='application/json')


class WebStream(object):

    def __init__(self, request):
        self._request = request

    async def prepare(self, request):
        self._response = web.StreamResponse(headers={'Content-Type': 'text/plain'})
        await self._response.prepare(self._request)

    def closing(self):
        return self._request.protocol.transport.is_closing()

    async def write(self, data):
        await self._response.write(data)

    async def finish(self):
        return await self._response.write_eof()


class AE5K8SHandler(object):
    def __init__(self, url, token, prometheus_url=None):
        self.xfrm = AE5K8STransformer(url, token)
        if prometheus_url:
            self.promql = AE5PromQLTransformer(prometheus_url, token)

    @classmethod
    def get_promQL_IP(cls, url, token):
        "Get the IP for the prometheus-k8s service"
        session = requests.Session()
        session.verify = False
        session.headers['Authorization'] = f'Bearer {token}'
        resp = session.get(DEFAULT_K8S_URL + 'api/v1/namespaces/monitoring/services/')
        entries = [el for el in resp.json()['items']
                   if el['metadata']['name'] == 'prometheus-k8s']
        assert len(entries) == 1, "More than one prometheus-k8s service found"
        return entries[0]['spec']['clusterIP']

    async def cleanup(self):
        await self.xfrm.close()

    async def hello(self, request):
        return web.Response(text="Alive and kicking")

    async def nodeinfo(self, request):
        result = await self.xfrm.node_info()
        return _json(result)

    async def _podinfo(self, ids, quiet=False):
        is_single = isinstance(ids, str)
        idset = [ids] if is_single else ids
        results = await self.xfrm.pod_info(idset, return_exceptions=True)
        invalid = [id for id, q in zip(idset, results) if isinstance(q, Exception)]
        if invalid and not quiet:
            plural = "s" if len(invalid) > 1 else ""
            raise web.HTTPUnprocessableEntity(reason=f'Invalid or missing ID{plural}: {", ".join(invalid)}')
        if is_single:
            return results[0]
        values = {id: q for id, q in zip(idset, results) if not isinstance(q, Exception)}
        return values

    async def podinfo_get_query(self, request):
        if not request.query:
            raise web.HTTPUnprocessableEntity(reason='Must supply an ID')
        invalid_keys = set(k for k in request.query if k != 'id')
        if invalid_keys:
            query = urlencode(request.query)
            raise web.HTTPUnprocessableEntity(reason=f'Invalid query: {query}')
        result = await self._podinfo(list(request.query.values()), True)
        return _json(result)

    async def podinfo_post(self, request):
        try:
            data = await request.json()
        except json.decoder.JSONDecodeError:
            data = None
        if not isinstance(data, list):
            raise web.HTTPUnprocessableEntity(reason='Must be a list of IDs')
        result = await self._podinfo(data, True)
        return _json(result)

    async def podinfo_get_path(self, request):
        return _json(await self._podinfo(request.match_info['id']))

    async def podlog(self, request):
        id = request.match_info['id']
        if 'container' in request.query:
            container = ','.join(v for k, v in request.query.items() if k == 'container')
        else:
            container = None
        if 'follow' in request.query:
            values = [v for k, v in request.query.items() if k == 'follow']
            value = ','.join(values)
            if value not in ('', 'true', 'false'):
                raise web.HTTPUnprocessableEntity(reason=f'Invalid parameter: follow={values[0]}')
            follow = value != 'false'
        else:
            follow = False
        try:
            await self.xfrm.pod_log(id, container, follow=follow, stream=WebStream(request))
        except (KeyError, ValueError) as exc:
            raise web.HTTPUnprocessableEntity(reason=str(exc))

    async def promql_status(self, request):
        if self.promql is None:
            raise web.HTTPMethodNotAllowed(reason="AE5 instance does not expose PromQL service.")
        return web.Response(text="Alive and kicking")

    async def query_range(self, request):
        await self.promql_status(request)
        if not ('query' in request.query or ('id' in request.query and 'metric' in request.query)):
            raise web.HTTPUnprocessableEntity(reason='Must supply an ID and metric or an explicit query.')
        valid = ('id', 'query', 'metric', 'start', 'end', 'step', 'samples', 'period')
        invalid_keys = set(k for k in request.query if k not in valid)
        if invalid_keys:
            query = urlencode(request.query)
            raise web.HTTPUnprocessableEntity(reason=f'Invalid query: {query}')

        query = dict(request.query)
        pod_id = request.query.pop('id', None)
        resp = await self.promql.query_range(pod_id, **query)
        if resp['status'] == 'success':
            result = resp['data']['result']
            return _json(result[0]['values'] if len(result) else [])
        raise web.HTTPUnprocessableEntity(reason=f'Prometheus query returned status {resp["status"]}.')


def main(url=None, token=None, port=None, promql_port=None):
    url = url or os.environ.get('AE5_K8S_URL', DEFAULT_K8S_URL)
    if token is None:
        token = os.environ.get('AE5_K8S_TOKEN')
    if token is None:
        token_file = os.environ.get('AE5_K8S_TOKEN_FILE', DEFAULT_K8S_TOKEN_FILE)
        if token_file and os.path.exists(token_file):
            with open(token_file, 'r') as fp:
                token = fp.read().strip()
    if promql_port is None:
        promql_port = os.environ.get('PROMETHEUS_PORT', DEFAULT_PROMETHEUS_PORT)

    try:
        promql_ip = AE5K8SHandler.get_promQL_IP(url, token)
        promql_url = f'http://{promql_ip}:{promql_port}'
    except Exception:
        promql_url = None

    app = web.Application()
    handler = AE5K8SHandler(url, token, promql_url)
    app.add_routes([web.get('/', handler.hello),
                    web.get('/__status__', handler.hello),
                    web.get('/nodes', handler.nodeinfo),
                    web.get('/pods', handler.podinfo_get_query),
                    web.post('/pods', handler.podinfo_post),
                    web.get('/pod/{id}', handler.podinfo_get_path),
                    web.get('/pod/{id}/log', handler.podlog),
                    web.get('/promql/', handler.promql_status),
                    web.get('/promql/__status__', handler.promql_status),
                    web.get('/promql/query_range', handler.query_range)])
    port = port or int(os.environ.get('AE5_K8S_PORT') or '8086')
    web.run_app(app, port=port)


if __name__ == '__main__':
    url = sys.argv[1] if len(sys.argv) > 1 else None
    if url and url.startswith('ssh:'):
        username, hostname = url[4:].split('@', 1)
        proc, url = tunneled_k8s_url(hostname, username)
    main(url=url, token=False if url else None)
