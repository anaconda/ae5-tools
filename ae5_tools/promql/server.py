import asyncio
import datetime
import json
import os
import re
import sys

import aiohttp
import requests

from aiohttp import web
from urllib.parse import urlencode

from ..k8s.server import DEFAULT_K8S_URL, DEFAULT_K8S_TOKEN_FILE

DEFAULT_PROMETHEUS_PORT = 9090


def _json(result):
    text = json.dumps(result, indent=2)
    return web.Response(text=text, content_type='application/json')


class AE5PromQLHandler(object):

    def __init__(self, url, token):
        self.xfrm = AE5PromQLTransformer(url, token)

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

    async def hello(self, request):
        return web.Response(text="Alive and kicking")

    async def query_range(self, request):
        if not ('query' in request.query or ('id' in request.query and 'metric' in request.query)):
            raise web.HTTPUnprocessableEntity(reason='Must supply an ID and metric or an explicit query.')
        valid = ('id', 'query', 'metric', 'start', 'end', 'step', 'samples', 'period')
        invalid_keys = set(k for k in request.query if k not in valid)
        if invalid_keys:
            query = urlencode(request.query)
            raise web.HTTPUnprocessableEntity(reason=f'Invalid query: {query}')

        # Compute date range and step
        if 'period' in request.query:
            timedelta = parse_timedelta(request.query['period'])
        else:
            timedelta = datetime.timedelta(weeks=4)
        end = request.query.get('end', datetime.datetime.utcnow())
        start = request.query.get('start', end-timedelta)
        end_timestamp = end.isoformat("T") + "Z"
        start_timestamp = start.isoformat("T") + "Z"
        if 'step' in request.query:
            step = request.query['step']
        else:
            samples = int(request.query.get('samples', 200))
            step = int(((end-start)/samples).total_seconds())

        metric = request.query.get('metric', None)
        query = request.query.get('query', None)
        pod_id = request.query.get('id', None)

        resp = await self.xfrm.query_range(query, pod_id, metric, start_timestamp, end_timestamp, step)
        if resp['status'] == 'success':
            result = resp['data']['result']
            return _json(result[0]['values'] if len(result) else [])
        raise web.HTTPUnprocessableEntity(reason=f'Prometheus query returned status {resp["status"]}.')


_period_regex = re.compile(r'((?P<weeks>\d+?)w)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?')


def parse_timedelta(time_str):
    parts = _period_regex.match(time_str)
    if not parts:
        return
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.items():
        if param:
            time_params[name] = int(param)
    return dt.timedelta(**time_params)


class AE5PromQLTransformer(object):

    def __init__(self, url=None, token=None):
        headers = {'accept': 'application/json'}
        if token:
            headers['authorization'] = f'Bearer {token}'
        self._headers = headers
        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))
        self._url = url.rstrip('/')

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    def __del__(self):
        if self._session is not None:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self.close())

    async def get(self, path, type='json', ok404=False):
        if not path.startswith('/'):
            path = '/api/v1/' + path
        url = self._url + path
        resp = await self._session.get(url, headers=self._headers)
        if resp.status == 404 and ok404:
            return
        resp.raise_for_status()
        if type == 'json':
            return await resp.json()
        elif type == 'text':
            return await resp.text()
        else:
            return resp

    async def query_range(self, query, pod_id, metric, start, end, step):
        if query is None:
            regex = f'anaconda-app-{pod_id}-.*'
            query = f"{metric}{{container_name='app',pod_name=~'{regex}'}}"
        url = f'query_range?query={query}&start={start}&end={end}&step={step}'
        return await self.get(url)


def main(url=None, token=None, port=None, promql_port=None):
    url = url or os.environ.get('AE5_K8S_URL', DEFAULT_K8S_URL)
    port = port or int(os.environ.get('AE5_PROMQL_PORT') or '8086')
    if token is None:
        token = os.environ.get('AE5_K8S_TOKEN')
    if token is None:
        token_file = os.environ.get('AE5_K8S_TOKEN_FILE', DEFAULT_K8S_TOKEN_FILE)
        if token_file and os.path.exists(token_file):
            with open(token_file, 'r') as fp:
                token = fp.read().strip()
    if promql_port is None:
        promql_port = os.environ.get('PROMETHEUS_PORT', DEFAULT_PROMETHEUS_PORT)

    promql_ip = AE5PromQLHandler.get_promQL_IP(url, token)
    promql_url = f'http://{promql_ip}:{promql_port}'

    app = web.Application()
    handler = AE5PromQLHandler(promql_url, token)
    app.add_routes([web.get('/', handler.hello),
                    web.get('/__status__', handler.hello),
                    web.get('/query_range', handler.query_range)])
    web.run_app(app, port=port)


if __name__ == '__main__':
    url = sys.argv[1] if len(sys.argv) > 1 else None
    main(url=url, token=False if url else None)
