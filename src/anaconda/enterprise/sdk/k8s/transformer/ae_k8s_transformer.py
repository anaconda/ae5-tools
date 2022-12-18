import asyncio
import io
import json
import re
from urllib.parse import urlencode

import aiohttp

from .ae_base_transformer import AEBaseTransformer
from .constants import FIELD_RENAMES
from .file_stream import FileStream
from .utils import _k8s_pod_to_record, _or_raise, _pod_merge_metrics, _to_float, _to_text, _to_text2


class AEK8STransformer(AEBaseTransformer):
    async def has_metrics(self):
        if self.has_metrics is None:
            result = await self.get("/apis/metrics.k8s.io/v1beta1", ok404=True)
            self.has_metrics = result is not None
        return self.has_metrics

    async def _pod_info(self, id, return_exceptions=False):
        if not re.match(r"[a-f0-9]{2}-[a-f0-9]{32}", id) or not id.startswith(("a1", "a2")):
            return _or_raise(ValueError(f"Invalid ID: {id}"), return_exceptions)
        prefix, slug = id.split("-", 1)
        if prefix == "a1":
            queries = (f"anaconda-session-id={slug}",)
        else:
            queries = (f"anaconda-app-id={slug}", f"job-name=anaconda-job-{slug}")
        for query in queries:
            query = urlencode({"labelSelector": query, "limit": 1})
            path = f"namespaces/default/pods?{query}"
            resp1 = await self.get(path)
            if isinstance(resp1, dict) and resp1.get("items"):
                return _k8s_pod_to_record(resp1["items"][0])
        else:
            return _or_raise(KeyError(f"Pod not found: {id}"), return_exceptions)

    async def _exec_pod(self, pod, namespace, container, command):
        await self.connect()
        path = f"/api/v1/namespaces/{namespace}/pods/{pod}/exec"
        params = {
            "command": command,
            "container": container,
            "stdout": True,
            "stderr": True,
            "stdin": False,
            "tty": False,
        }
        headers = {"sec-websocket-protocol": "v4.channel.k8s.io"}
        if "authorization" in self.headers:
            headers["authorization"] = self.headers["authorization"]
        url = "{}{}?{}".format(self.url, path, urlencode(params, True))
        output = {}
        async with self.session.ws_connect(url, headers=headers) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    output.setdefault(msg.data[0], []).append(msg.data[1:])
        output = {k: b"".join(v).decode("utf-8", errors="replace") for k, v in output.items()}
        if 3 in output:
            output[3] = json.loads(output[3])
            if output[3].get("status") != "Success":
                msg = ["Unexpected error executing task"]
                msg.append(f"Pod: {pod}  Container: {container}")
                msg.append("Command: {}".format(" ".join(command)))
                if output.get(1):
                    msg.append("--- STDOUT ---")
                    msg.append(output[1].rstrip())
                if output.get(2):
                    msg.append("--- STDERR ---")
                    msg.append(output[2].rstrip())
                raise RuntimeError("\n".join(msg))
        return output

    async def _pod_changes(self, data):
        cmd = [
            "/bin/sh",
            "-c",
            (
                "cd /opt/continuum/project;"
                'find . -name .git -prune -o  -printf "%T+ %p\\n";'
                "echo ----;"
                "git status --porcelain || /bin/true"
            ),
        ]
        result = {"modified": [], "deleted": [], "added": [], "mtime": None}
        try:
            output = await self._exec_pod(data["name"], "default", data["containers"]["sync"]["name"], cmd)
        except RuntimeError:
            return result
        found = False
        gitkeys = {" D": "deleted", "??": "added"}
        for line in output.get(1, "").splitlines():
            if not line:
                continue
            elif line.startswith("-"):
                found = True
            elif found:
                mode, path = line[:2], line[3:]
                result[gitkeys.get(mode, "modified")].append(path)
            else:
                result["mtime"] = max(result.get("mtime") or "", line.split()[0])
        return result

    async def pod_info(self, id, return_exceptions=False):
        if isinstance(id, list):
            return await asyncio.gather(*(self.pod_info(t) for t in id), return_exceptions=return_exceptions)
        nrec = await self._pod_info(id, return_exceptions=return_exceptions)
        if isinstance(nrec, Exception):
            return nrec
        name = nrec["name"]
        if await self.has_metrics():
            url = f"/apis/metrics.k8s.io/v1beta1/namespaces/default/pods/{name}"
        else:
            url = f"namespaces/monitoring/services/heapster/proxy/apis/metrics/v1alpha1/namespaces/default/pods/{name}"
        if id.startswith("a2-"):
            resp2, resp3 = await self.get(url, ok404=True), None
        else:
            resp2, resp3 = await asyncio.gather(self.get(url, ok404=True), self._pod_changes(nrec))
        _pod_merge_metrics(nrec, resp2)
        if resp3 is not None:
            nrec["changes"] = resp3
        return nrec

    async def pod_log(self, id, container=None, follow=False, stream=None):
        data = await self._pod_info(id)
        if not container:
            container = "editor" if id.startswith("a1-") else "app"
        if container not in data["containers"]:
            keys = ", ".join(sorted(data["containers"].keys()))
            raise KeyError(f"Container must be one of: {keys}")
        pname = data["name"]
        cname = data["containers"][container]["name"]
        if follow and (stream is None or isinstance(stream, io.TextIOWrapper)):
            stream = FileStream(stream)
        follow = str(bool(follow)).lower()
        path = f"namespaces/default/pods/{pname}/log?container={cname}&follow={follow}"
        ctype = "text" if stream is None else "content"
        result = await self.get(path, type=ctype)
        if ctype == "text":
            return result
        await stream.prepare(result)
        async for data, eoc in result.content.iter_chunks():
            if stream.closing():
                await result.release()
                break
            await stream.write(data)
        await stream.finish()

    async def node_info(self):
        resp1 = self.get("nodes")
        resp2 = self.get("pods")
        if await self.has_metrics():
            url = "/apis/metrics.k8s.io/v1beta1/pods"
        else:
            url = "namespaces/monitoring/services/heapster/proxy/apis/metrics/v1alpha1/pods"
        resp3 = self.get(url)
        resp1, resp2, resp3 = await asyncio.gather(resp1, resp2, resp3)
        resp1, resp2, resp3 = resp1["items"], resp2["items"], resp3["items"]

        nodeMap = {}
        nodeList = []
        subsets = ("total", "sessions", "deployments", "middleware", "system")
        whiches = ("requests", "limits", "usage")
        for rec in resp1:
            nodeRec = {
                "name": rec["metadata"]["name"],
                "role": rec["metadata"]["labels"]["role"],
                "capacity": {
                    "pods": rec["status"]["allocatable"]["pods"],
                    "mem": _to_text2(rec["status"]["allocatable"]["memory"]),
                    "cpu": rec["status"]["allocatable"]["cpu"],
                    "gpu": rec["status"]["allocatable"].get("nvidia.com/gpu", "0"),
                },
                "ready": any(c["type"] == "Ready" and c["status"] == "True" for c in rec["status"]["conditions"]),
                "conditions": [
                    c["type"] for c in rec["status"]["conditions"] if c["type"] != "Ready" and c["status"] == "True"
                ],
                "timestamp": None,
                "window": None,
            }
            for subset in subsets:
                srec = nodeRec[subset] = {"pods": 0, "pending": 0}
                for which in whiches:
                    srec[which] = {"mem": 0, "cpu": 0, "gpu": 0}
            nodeMap[nodeRec["name"]] = nodeRec
            nodeList.append(nodeRec)

        podMap = {}
        for pod in resp2:
            nodeName = pod["spec"]["nodeName"]
            phase = pod["status"]["phase"]
            if phase in ("Failed", "Succeeded") or nodeName not in nodeMap:
                continue
            pfld = "pending" if phase == "Pending" else "pods"
            podName = pod["metadata"]["name"]
            if podName.startswith("anaconda-session"):
                t_sub = "sessions"
            elif podName.startswith("anaconda-app"):
                t_sub = "deployments"
            elif podName.startswith("anaconda-"):
                t_sub = "middleware"
            else:
                t_sub = "system"
            nodeRec = nodeMap[nodeName]
            podMap[podName] = [nodeRec, t_sub]
            for subset in ("total", t_sub):
                subRec = nodeRec[subset]
                subRec[pfld] += 1
                for container in pod["spec"]["containers"]:
                    for which in ("requests", "limits"):
                        src = container["resources"].get(which, {})
                        dst = subRec[which]
                        for key, value in dst.items():
                            skey = FIELD_RENAMES.get(key, key)
                            default = "inf" if which == "limits" and key != "gpu" else "0"
                            dst[key] = value + _to_float(src.get(skey, src.get(key, default)))
                subRec["usage"]["gpu"] = subRec["requests"]["gpu"]

        for pod in resp3:
            podName = pod["metadata"]["name"]
            if podName in podMap:
                nodeRec, t_sub = podMap[podName]
                if nodeRec["window"] is None:
                    nodeRec["window"] = pod["window"]
                if nodeRec["timestamp"] is None:
                    nodeRec["timestamp"] = pod["timestamp"]
                for subset in ("total", t_sub):
                    subRec = nodeRec[subset]
                    dst = subRec["usage"]
                    for container in pod["containers"]:
                        uRec = container["usage"]
                        for key, value in dst.items():
                            skey = FIELD_RENAMES.get(key, key)
                            dst[key] += _to_float(uRec.get(skey, uRec.get(key, "0")))

        for nodeRec in nodeList:
            for subset in subsets:
                for which in whiches:
                    dst = nodeRec[subset][which]
                    for key, value in dst.items():
                        dst[key] = _to_text(value)

        return nodeList
