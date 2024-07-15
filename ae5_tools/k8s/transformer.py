import asyncio
import datetime
import io
import json
import re
import sys
from urllib.parse import urlencode

import aiohttp
import dateutil.parser


def _or_raise(exc, return_exceptions):
    if return_exceptions:
        return exc
    raise exc


def _to_datetime(rec):
    for key, value in rec.items() if isinstance(rec, dict) else enumerate(rec):
        if isinstance(value, str):
            if re.match(
                r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3_]|[01][0-9]):[0-5][0-9])?$",
                value,
            ):
                rec[key] = dateutil.parser.parse(value)
        elif isinstance(value, (list, dict)):
            _to_datetime(value)
    return rec


def _to_float(text):
    if isinstance(text, dict):
        return {k: _to_float(v) for k, v in text.items()}
    elif not isinstance(text, str):
        return text
    match = re.match(r"^([0-9]+(?:[.][0-9]*)?|inf)\s*(m|Ki|Mi|Gi|Ti)?$", text)
    if not match:
        return text
    value, suffix = match.groups()
    value = float(value)
    if suffix == "":
        return value
    elif suffix == "m":
        return value / 1000.0
    elif suffix == "Ki":
        return value * 1000.0
    elif suffix == "Mi":
        return value * 1.0e6
    elif suffix == "Gi":
        return value * 1.0e9
    elif suffix == "Ti":
        return value * 1.0e9
    else:
        return value


def _to_text(value):
    if isinstance(value, dict):
        return {k: _to_text(v) for k, v in value.items()}
    elif not isinstance(value, (int, float)):
        return value
    if not value:
        return "0"
    elif value == float("inf"):
        return "inf"
    elif value < 1.0:
        mult, suffix = 0.001, "m"
    elif value > 1.0e12:
        mult, suffix = 1.0e12, "Ti"
    elif value > 1.0e9:
        mult, suffix = 1.0e9, "Gi"
    elif value > 1.0e6:
        mult, suffix = 1.0e6, "Mi"
    elif value > 1.0e3:
        mult, suffix = 1.0e3, "Ki"
    else:
        mult, suffix = 1.0, ""
    value /= mult
    if suffix == "m":
        value = str(int(value + 0.5))
    elif value < 10.0:
        value = f"{value:.3f}"
    elif value < 100.0:
        value = f"{value:.2f}"
    else:
        value = f"{value:.1f}"
    return value + suffix


def _to_text2(value):
    return _to_text(_to_float(value))


FIELD_RENAMES = {"gpu": "nvidia.com/gpu", "mem": "memory"}


def _k8s_pod_to_record(pRec):
    if isinstance(pRec, list):
        return [_k8s_pod_to_record(rec) for rec in pRec]
    npRec = {
        "name": pRec["metadata"]["name"],
        "node": pRec["spec"]["nodeName"],
        "phase": pRec["status"]["phase"],
        "since": max(c["lastTransitionTime"] or "" for c in pRec["status"]["conditions"]),
        "restarts": 0,
        "containers": {},
        "requests": {"mem": 0, "cpu": 0, "gpu": 0},
        "limits": {"mem": 0, "cpu": 0, "gpu": 0},
    }
    cMap = {}
    for cRec in pRec["status"]["containerStatuses"]:
        name = cRec["name"]
        if name == "app":
            cid = "app"
        elif "proxy" in name:
            cid = "proxy"
        elif "sync" in name:
            cid = "sync"
        else:
            cid = "editor"
        ncRec = {
            "name": name,
            "ready": cRec["ready"],
            "since": cRec["state"].get("running", {}).get("startedAt"),
            "restarts": cRec["restartCount"],
        }
        npRec["restarts"] = max(npRec["restarts"], ncRec["restarts"])
        npRec["containers"][cid] = cMap[name] = ncRec
    for which in ("requests", "limits"):
        dst = npRec[which]
        default = float("inf") if which == "limits" else 0
        for cRec in pRec["spec"]["containers"]:
            ncRec = cMap[cRec["name"]]
            src = ncRec[which] = cRec["resources"][which]
            for key, value in dst.items():
                skey = FIELD_RENAMES.get(key, key)
                dst[key] = value + _to_float(src.get(skey, src.get(key, default)))
        for key, value in dst.items():
            dst[key] = _to_text(value)
    return npRec


def _pod_merge_metrics(pRec, mRec):
    mRec = mRec or {}
    pRec["window"] = mRec.get("window")
    pRec["timestamp"] = mRec.get("timestamp")
    cMap = {c["name"]: c for c in pRec["containers"].values()}
    dst = pRec["usage"] = {"mem": 0, "cpu": 0, "gpu": 0}
    for mcRec in mRec.get("containers", ()):
        cRec = cMap.get(mcRec["name"])
        src = cRec["usage"] = mcRec["usage"]
        src["gpu"] = cRec["requests"]["nvidia.com/gpu"]
        for key, value in dst.items():
            skey = FIELD_RENAMES.get(key, key)
            dst[key] = value + _to_float(src.get(skey, src.get(key, "0")))
    for cRec in pRec["containers"].values():
        cRec.setdefault("usage", {})
        for field in ("mem", "cpu", "gpu"):
            cRec["usage"].setdefault(field, "0")
    for key, value in dst.items():
        dst[key] = _to_text(value)


_period_regex = re.compile(r"((?P<weeks>\d+?)w)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?")


def parse_timedelta(time_str):
    parts = _period_regex.match(time_str)
    if not parts:
        return
    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = int(param)
    return datetime.timedelta(**time_params)


class FileStream(object):
    def __init__(self, stream):
        self.stream = sys.stdout if stream is None else stream

    async def prepare(self, request):
        pass

    def closing(self):
        return False

    async def write(self, data):
        return self.stream.write(data.decode())

    async def finish(self):
        pass


class AE5BaseTransformer(object):
    def __init__(self, url=None, token=None, namespace=None):
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        self._ns = namespace or "default"
        self._headers = headers
        self._session = None
        self._url = url.rstrip("/")
        self._metrics_url = None

    async def connect(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    def __del__(self):
        if self._session is not None:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self.close())

    async def get(self, path, type="json", ok404=False, ok403=False):
        await self.connect()
        if not path.startswith("/"):
            path = "/api/v1/" + path
        url = self._url + path
        async with self._session.get(url, headers=self._headers) as resp:
            if resp.status == 404 and ok404:
                return
            if resp.status == 403 and ok403:
                return
            resp.raise_for_status()
            if type == "json":
                return await resp.json()
            elif type == "text":
                return await resp.text()
            else:
                return resp


class AE5K8STransformer(AE5BaseTransformer):
    async def metrics_url(self):
        if self._metrics_url is None:
            for url in ("/apis/metrics.k8s.io/v1beta1", "namespaces/monitoring/services/heapster/proxy/apis/metrics/v1alpha1"):
                resp = await self.get(url, ok404=True, ok403=True)
                if resp is not None:
                    self._metrics_url = f"{url}/namespaces/{self._ns}/pods"
                    break
            else:
                self._metrics_url = ""
        return self._metrics_url

    async def _pod_info(self, id, return_exceptions=False):
        if not re.match(r"[a-f0-9]{2}-[a-f0-9]{32}", id) or not id.startswith(("a1", "a2")):
            return _or_raise(ValueError(f"Invalid ID: {id}"), return_exceptions)
        prefix, slug = id.split("-", 1)
        if prefix == "a1":
            queries = (f"anaconda-session-id={slug}", f"session-id={slug}")
        else:
            queries = (f"app-id={slug}", f"anaconda-app-id={slug}", f"job-id={slug}", f"job-name=anaconda-job-{slug}")
        for query in queries:
            query = urlencode({"labelSelector": query, "limit": 1})
            path = f"namespaces/{self._ns}/pods?{query}"
            resp1 = await self.get(path)
            if isinstance(resp1, dict) and resp1.get("items"):
                return _k8s_pod_to_record(resp1["items"][0])
        else:
            return _or_raise(KeyError(f"Pod not found: {id}"), return_exceptions)

    async def _exec_pod(self, pod, container, command):
        await self.connect()
        path = f"/api/v1/namespaces/{self._ns}/pods/{pod}/exec"
        params = {
            "command": command,
            "container": container,
            "stdout": True,
            "stderr": True,
            "stdin": False,
            "tty": False,
        }
        headers = {"sec-websocket-protocol": "v4.channel.k8s.io"}
        if "authorization" in self._headers:
            headers["authorization"] = self._headers["authorization"]
        url = "{}{}?{}".format(self._url, path, urlencode(params, True))
        output = {}
        async with self._session.ws_connect(url, headers=headers) as ws:
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
            ("cd /opt/continuum/project;" 'find . -name .git -prune -o  -printf "%T+ %p\\n";' "echo ----;" "git status --porcelain || /bin/true"),
        ]
        result = {"modified": [], "deleted": [], "added": [], "mtime": None}
        try:
            output = await self._exec_pod(data["name"], data["containers"]["sync"]["name"], cmd)
        except Exception as exc:
            print("UNEXPECTED ERROR IN EXEC_POD:", type(exc), exc)
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
        url = await self.metrics_url()
        # self._pod_changes is not working and it's not clear how long that has been the case.
        # for now we are skipping it. It relies on _pod_exec and websockets. To debug it, put
        # the original gather code back.
        resp2 = self.get(f"{url}/{name}", ok404=True, ok403=True) if url else self._none()
        # resp3 = self._none() if id.startswith("a2-") else self._pod_changes(nrec)
        # resp2, resp3 = await asyncio.gather(resp2, resp3)
        resp2, resp3 = await resp2, None
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
        path = f"namespaces/{self._ns}/pods/{pname}/log?container={cname}&follow={follow}"
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

    async def _empty_list(self):
        return {"items": []}

    async def _none(self):
        return None

    async def node_info(self):
        resp1 = self.get("nodes")
        resp2 = self.get("pods")
        url = await self.metrics_url()
        resp3 = self.get(url) if url else self._empty_list()
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
                "conditions": [c["type"] for c in rec["status"]["conditions"] if c["type"] != "Ready" and c["status"] == "True"],
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


class AE5PromQLTransformer(AE5BaseTransformer):
    async def query_range(self, pod_id=None, query=None, metric=None, start=None, end=None, step=None, period=None, samples=None):
        if period is None:
            timedelta = datetime.timedelta(weeks=4)
        else:
            timedelta = parse_timedelta(period)
        end = end or datetime.datetime.utcnow()
        start = start or (end - timedelta)
        end_timestamp = end.isoformat("T") + "Z"
        start_timestamp = start.isoformat("T") + "Z"
        if step is None:
            samples = int(samples or 200)
            step = int(((end - start) / samples).total_seconds())
        if query is None:
            regex = f"anaconda-app-{pod_id}-.*"
            query = f"{metric}{{container_name='app',pod_name=~'{regex}'}}"
        url = f"query_range?query={query}&start={start_timestamp}&end={end_timestamp}&step={step}"
        return await self.get(url)
