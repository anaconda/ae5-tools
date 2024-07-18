import datetime
import json
import os
import re
import sys
from urllib.parse import unquote, urlencode

import requests
from aiohttp import web

from .ssh import tunneled_k8s_url
from .transformer import AE5K8STransformer, AE5PromQLTransformer

DEFAULT_K8S_URL = "https://kubernetes.default/"
DEFAULT_K8S_TOKEN_FILES = (
    "/var/run/secrets/kubernetes.io/serviceaccount/token",
    "/var/run/secrets/user_credentials/k8s_token",
)
K8S_ENDPOINT_PORT = int(os.environ.get("AE5_K8S_PORT") or "8086")
DEFAULT_PROMETHEUS_PORT = 9090


def _json(result):
    text = json.dumps(result, indent=2)
    return web.Response(text=text, content_type="application/json")


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


class AE5K8SHandler(object):
    def __init__(self, url, token, namespace, prometheus_url=None):
        self.xfrm = AE5K8STransformer(url, token, namespace)
        if prometheus_url:
            self.promql = AE5PromQLTransformer(prometheus_url, token)

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


def main(url=None, token=None, namespace=None, port=None, promql_port=None):
    if url:
        print("API url supplied as argument")
    elif os.environ.get("AE5_K8S_URL"):
        url = os.environ.get("AE5_K8S_URL")
        print("API url supplied as AE5_K8S_URL")
    else:
        url = DEFAULT_K8S_URL
        print("API url default value used")
    if token is False:
        print("API token not required")
    elif token:
        print("API token supplied as argument")
    elif os.environ.get("AE5_K8S_TOKEN"):
        token = os.environ.get("AE5_K8S_TOKEN")
        print("API token supplied by AE5_K8S_TOKEN")
    if namespace:
        print("Namespace supplied as argument")
    elif os.environ.get("AE5_K8S_NAMESPACE"):
        namespace = os.environ.get("AE5_K8S_NAMESPACE")
        print("Namespace supplied by AE5_K8S_NAMESPACE")
    for token_file in (os.environ.get("AE5_K8S_TOKEN_FILE"),) + DEFAULT_K8S_TOKEN_FILES:
        if not token_file:
            continue
        if token is None and os.path.exists(token_file):
            print("API token supplied in file:", token_file)
            with open(token_file, "r") as fp:
                token = fp.read().strip()
        ns_file = re.sub(r"([_/])token$", r"\1namespace", token_file)
        if namespace is None and os.path.exists(ns_file):
            print("Namespace supplied in file:", ns_file)
            with open(ns_file, "r") as fp:
                namespace = fp.read().strip()
    print("API url:", url)
    print("API token:", "found" if token else "empty")
    print("Namespace:", namespace)

    if promql_port is None:
        promql_port = os.environ.get("PROMETHEUS_PORT", DEFAULT_PROMETHEUS_PORT)

    try:
        promql_ip = AE5K8SHandler.get_promQL_IP(url, token)
        promql_url = f"http://{promql_ip}:{promql_port}"
    except Exception:
        promql_url = None

    app = web.Application()
    handler = AE5K8SHandler(url, token, namespace, promql_url)
    app.add_routes(
        [
            web.get("/", handler.hello),
            web.get("/__status__", handler.hello),
            web.get("/nodes", handler.nodeinfo),
            web.get("/pods", handler.podinfo_get_query),
            web.post("/pods", handler.podinfo_post),
            web.get("/pod/{id}", handler.podinfo_get_path),
            web.get("/promql/", handler.promql_status),
            web.get("/promql/__status__", handler.promql_status),
            web.get("/promql/query_range", handler.query_range),
            web.get("/pod/{id}/log", handler.podlog),
        ]
    )
    port = port or int(os.environ.get("AE5_K8S_PORT") or "8086")
    web.run_app(app, port=port or K8S_ENDPOINT_PORT)


if __name__ == "__main__":
    url = None
    skip = False
    for arg in sys.argv[1:]:
        if skip or arg.startswith("--"):
            skip = not (skip or "=" in arg)
            continue
        if url is not None:
            raise RuntimeError("No more than one positional argument expected")
        url = arg
    if url and url.startswith("ssh:"):
        username, hostname = url[4:].split("@", 1)
        proc, url = tunneled_k8s_url(hostname, username)
    main(url=url, token=False if url else None)
