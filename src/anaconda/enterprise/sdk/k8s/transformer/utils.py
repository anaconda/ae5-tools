import datetime
import re

import dateutil.parser

from .constants import FIELD_RENAMES


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
        elif name == "app-proxy":
            cid = "proxy"
        elif name.startswith("tool-anaconda-platform-sync-"):
            cid = "sync"
        elif not name.startswith("tool-proxy-"):
            cid = "editor"
        elif name.rsplit("-", 1)[-1] in [
            c["name"].rsplit("-", 1)[-1]
            for c in pRec["spec"]["containers"]
            if c["name"].startswith("tool-anaconda-platform-sync-")
        ]:
            cid = "sync-proxy"
        else:
            cid = "proxy"
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


_period_regex = re.compile(
    r"((?P<weeks>\d+?)w)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?"
)


def parse_timedelta(time_str):
    parts = _period_regex.match(time_str)
    if not parts:
        return
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.items():
        if param:
            time_params[name] = int(param)
    return datetime.timedelta(**time_params)
