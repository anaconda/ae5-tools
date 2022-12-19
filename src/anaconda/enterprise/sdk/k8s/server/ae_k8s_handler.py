import datetime
import json
from urllib.parse import unquote, urlencode

import requests
from aiohttp import web

from ..transformer.ae_k8s_transformer import AEK8STransformer
from ..transformer.ae_promql_transformer import AEPromQLTransformer
from .constants import DEFAULT_K8S_URL
from .server import _json
from .web_stream import WebStream


class AEK8SHandler(object):
    def __init__(self, url, token, prometheus_url=None):
        self.xfrm = AEK8STransformer(url=url, token=token)
        if prometheus_url:
            self.promql = AEPromQLTransformer(url=prometheus_url, token=token)

    @classmethod
    def get_promQL_IP(cls, url, token):
        "Get the IP for the prometheus-k8s service"
        session = requests.Session()
        session.verify = False
        session.headers["Authorization"] = f"Bearer {token}"
        resp = session.get(DEFAULT_K8S_URL + "api/v1/namespaces/monitoring/services/")
        entries = [el for el in resp.json()["items"] if el["metadata"]["name"] == "prometheus-k8s"]
        assert len(entries) == 1, "More than one prometheus-k8s service found"
        return entries[0]["spec"]["clusterIP"]

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
            raise web.HTTPUnprocessableEntity(reason="Must supply an ID")
        invalid_keys = set(k for k in request.query if k != "id")
        if invalid_keys:
            query = urlencode(request.query)
            raise web.HTTPUnprocessableEntity(reason=f"Invalid query: {query}")
        result = await self._podinfo(list(request.query.values()), True)
        return _json(result)

    async def podinfo_post(self, request):
        try:
            data = await request.json()
        except json.decoder.JSONDecodeError:
            data = None
        if not isinstance(data, list):
            raise web.HTTPUnprocessableEntity(reason="Must be a list of IDs")
        result = await self._podinfo(data, True)
        return _json(result)

    async def podinfo_get_path(self, request):
        return _json(await self._podinfo(request.match_info["id"]))

    async def podlog(self, request):
        id = request.match_info["id"]
        if "container" in request.query:
            container = ",".join(v for k, v in request.query.items() if k == "container")
        else:
            container = None
        if "follow" in request.query:
            values = [v for k, v in request.query.items() if k == "follow"]
            value = ",".join(values)
            if value not in ("", "true", "false"):
                raise web.HTTPUnprocessableEntity(reason=f"Invalid parameter: follow={values[0]}")
            follow = value != "false"
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
        if not ("query" in request.query or ("id" in request.query and "metric" in request.query)):
            raise web.HTTPUnprocessableEntity(reason="Must supply an ID and metric or an explicit query.")
        valid = ("id", "query", "metric", "start", "end", "step", "samples", "period")
        invalid_keys = set(k for k in request.query if k not in valid)
        if invalid_keys:
            query = urlencode(request.query)
            raise web.HTTPUnprocessableEntity(reason=f"Invalid query: {query}")

        query = dict(request.query)
        pod_id = query.pop("id", None)
        if "start" in query:
            start = unquote(query["start"]).replace("Z", "")
            query["start"] = datetime.datetime.fromisoformat(start)
        if "end" in query:
            end = unquote(query["end"]).replace("Z", "")
            query["end"] = datetime.datetime.fromisoformat(end)
        resp = await self.promql.query_range(pod_id, **query)
        if resp["status"] == "success":
            result = resp["data"]["result"]
            return _json(result[0]["values"] if len(result) else [])
        raise web.HTTPUnprocessableEntity(reason=f'Prometheus query returned status {resp["status"]}.')
