import json
import os
import sys

from aiohttp import web

from ..ssh import tunneled_k8s_url
from .ae_k8s_handler import AEK8SHandler
from .constants import DEFAULT_K8S_TOKEN_FILES, DEFAULT_K8S_URL, DEFAULT_PROMETHEUS_PORT, K8S_ENDPOINT_PORT


def _json(result):
    text = json.dumps(result, indent=2)
    return web.Response(text=text, content_type="application/json")


def main(url=None, token=None, port=None, promql_port=None):
    url = url or os.environ.get("AE5_K8S_URL", DEFAULT_K8S_URL)
    if token is None:
        token = os.environ.get("AE5_K8S_TOKEN")
    if token is None:
        for token_file in (os.environ.get("AE5_K8S_TOKEN_FILE"),) + DEFAULT_K8S_TOKEN_FILES:
            if token_file and os.path.exists(token_file):
                print("Using Kubernetes API token:", token_file)
                with open(token_file, "r") as fp:
                    token = fp.read().strip()
                    break

    if promql_port is None:
        promql_port = os.environ.get("PROMETHEUS_PORT", DEFAULT_PROMETHEUS_PORT)

    try:
        promql_ip = AEK8SHandler.get_promQL_IP(url, token)
        promql_url = f"http://{promql_ip}:{promql_port}"
    except Exception:
        promql_url = None

    app = web.Application()
    handler = AEK8SHandler(url, token, promql_url)
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
