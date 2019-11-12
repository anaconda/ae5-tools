import os
import io
import re
import sys
import ast
import json
import aiohttp
import asyncio
from urllib.parse import urlencode
import dateutil.parser


def _or_raise(exc, return_exceptions):
    if return_exceptions:
        return exc
    raise exc


def _to_datetime(rec):
    for key, value in (rec.items() if isinstance(rec, dict) else enumerate(rec)):
        if isinstance(value, str):
            if re.match(r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3_]|[01][0-9]):[0-5][0-9])?$', value):
                rec[key] = dateutil.parser.parse(value)
        elif isinstance(value, (list, dict)):
            _to_datetime(value)
    return rec


def _to_float(text):
    match = re.match(r'^([0-9]+(?:[.][0-9]*)?|inf)\s*(m|Ki|Mi|Gi|Ti)?$', text)
    if not match:
        return text
    value, suffix = match.groups()
    value = float(value)
    if suffix == '':
        return value
    elif suffix == 'm':
        return value / 1000.0
    elif suffix == 'Ki':
        return value * 1000.0
    elif suffix == 'Mi':
        return value * 1.0e6
    elif suffix == 'Gi':
        return value * 1.0e9
    elif suffix == 'Ti':
        return value * 1.0e9
    else:
        return value


def _to_text(value):
    if not value:
        return '0'
    elif value == float('inf'):
        return 'inf'
    elif value < 1.0:
        mult, suffix = 0.001, 'm'
    elif value > 1.0e12:
        mult, suffix = 1.0e12, 'Ti'
    elif value > 1.0e9:
        mult, suffix = 1.0e9, 'Gi'
    elif value > 1.0e6:
        mult, suffix = 1.0e6, 'Mi'
    elif value > 1.0e3:
        mult, suffix = 1.0e3, 'Ki'
    else:
        mult, suffix = 1.0, ''
    value = int(value / (mult / 1.0e3) + 0.5) * 1.0e-3
    value = f'{value:.3f}'.rstrip('0').rstrip('.') + suffix
    return value


def _k8s_pod_to_record(pRec):
    if isinstance(pRec, list):
        return [_k8s_pod_to_record(rec) for rec in pRec]
    npRec = {'name': pRec['metadata']['name'],
             'node': pRec['spec']['nodeName'],
             'phase': pRec['status']['phase'],
             'since': max(c['lastTransitionTime'] or '' for c in pRec['status']['conditions']),
             'restarts': 0,
             'containers': {},
             'requests': {'memory': 0, 'cpu': 0, 'nvidia.com/gpu': 0},
             'limits': {'memory': 0, 'cpu': 0, 'nvidia.com/gpu': 0}}
    cMap = {}
    for cRec in pRec['status']['containerStatuses']:
        name = cRec['name']
        if name == 'app':
            cid = 'app'
        elif name == 'app-proxy':
            cid = 'proxy'
        elif name.startswith('tool-anaconda-platform-sync-'):
            cid = 'sync'
        elif not name.startswith('tool-proxy-'):
            cid = 'editor'
        elif name.rsplit('-', 1)[-1] in [c['name'].rsplit('-', 1)[-1]
                                         for c in pRec['spec']['containers']
                                         if c['name'].startswith('tool-anaconda-platform-sync-')]:
            cid = 'sync-proxy'
        else:
            cid = 'proxy'
        ncRec = {'name': name,
                 'ready': cRec['ready'],
                 'since': cRec['state'].get('running', {}).get('startedAt'),
                 'restarts': cRec['restartCount']}
        npRec['restarts'] = max(npRec['restarts'], ncRec['restarts'])
        npRec['containers'][cid] = cMap[name] = ncRec
    for which in ('requests', 'limits'):
        dst = npRec[which]
        default = float('inf') if which == 'limits' else 0
        for cRec in pRec['spec']['containers']:
            ncRec = cMap[cRec['name']]
            src = ncRec[which] = cRec['resources'][which]
            for key, value in dst.items():
                dst[key] = value + _to_float(src.get(key, default))
        for key, value in dst.items():
            dst[key] = _to_text(value)
    return npRec


def _pod_merge_metrics(pRec, mRec):
    mRec = mRec or {}
    pRec['window'] = mRec.get('window')
    pRec['timestamp'] = mRec.get('timestamp')
    cMap = {c['name']: c for c in pRec['containers'].values()}
    dst = pRec['usage'] = {'memory': 0, 'cpu': 0, 'nvidia.com/gpu': 0}
    for mcRec in mRec.get('containers', ()):
        cRec = cMap.get(mcRec['name'])
        src = cRec['usage'] = mcRec['usage']
        src['nvidia.com/gpu'] = cRec['requests']['nvidia.com/gpu']
        for key, value in dst.items():
            dst[key] = value + _to_float(src.get(key, "0"))
    for cRec in pRec['containers'].values():
        if 'usage' not in cRec:
            cRec['usage'] = {'memory': "0", 'cpu': "0", 'nvidia.com/gpu': "0"}
    for key, value in dst.items():
        dst[key] = _to_text(value)


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


class AE5K8STransformer(object):
    def __init__(self, url=None, token=None):
        headers = {'accept': 'application/json'}
        if token:
            headers['authorization'] = f'Bearer {token}'
        self._headers = headers
        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))
        self._url = url.rstrip('/') + '/api/v1/'

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    def __del__(self):
        if self._session is not None:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self.close())

    async def get(self, path, type='json', ok404=False):
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

    async def _pod_info(self, id, return_exceptions=False):
        if not re.match(r'[a-f0-9]{2}-[a-f0-9]{32}', id) or not id.startswith(('a1', 'a2')):
            return _or_raise(ValueError(f'Invalid ID: {id}'), return_exceptions)
        prefix, slug = id.split('-', 1)
        label = 'anaconda-session-id' if prefix == 'a1' else 'anaconda-app-id'
        query = urlencode({'labelSelector': f'{label}={slug}', 'limit': 1})
        path = f'namespaces/default/pods?{query}'
        resp1 = await self.get(path)
        resp1 = resp1['items']
        if len(resp1) != 1:
            return _or_raise(KeyError(f'Pod not found: {id}', return_exceptions))
        return _k8s_pod_to_record(resp1[0])
    
    async def _exec_pod(self, pod, namespace, container, command):
        path = f'namespaces/{namespace}/pods/{pod}/exec'
        params = {'command': command, 'container': container,
                  'stdout': True, 'stderr': True,
                  'stdin': False, 'tty': False}
        headers = {'sec-websocket-protocol': 'v4.channel.k8s.io'}
        if 'authorization' in self._headers:
            headers['authorization'] = self._headers['authorization']
        url = '{}{}?{}'.format(self._url, path, urlencode(params, True))
        output = {}
        async with self._session.ws_connect(url, headers=headers) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.BINARY:
                    output.setdefault(msg.data[0], []).append(msg.data[1:])
        output = {k: b''.join(v).decode('utf-8', errors='replace')
                  for k, v in output.items()}
        if 3 in output:
            output[3] = json.loads(output[3])
            if output[3].get('status') != 'Success':
                msg = ['Unexpected error executing task']
                msg.append(f'Pod: {pod}  Container: {container}')
                msg.append('Command: {}'.format(' '.join(command)))
                if output.get(1):
                    msg.append('--- STDOUT ---')
                    msg.append(output[1].rstrip())
                if output.get(2):
                    msg.append('--- STDERR ---')
                    msg.append(output[2].rstrip())
                raise RuntimeError('\n'.join(msg))
        return output

    async def _pod_changes(self, data):
        cmd = ['/bin/sh', '-c', ('cd /opt/continuum/project;'
                                 'find . -name .git -prune -o  -printf "%T+ %p\\n";'
                                 'echo ----;'
                                 'git status --porcelain || /bin/true')]
        result = {'modified': [], 'deleted': [], 'added': [], 'mtime': None}
        try:
            output = await self._exec_pod(data['name'], 'default', data['containers']['sync']['name'], cmd)
        except RuntimeError as exc:
            return result
        found = False
        gitkeys = {' D': 'deleted', '??': 'added'}
        for line in output.get(1, '').splitlines():
            if not line:
                continue
            elif line.startswith('-'):
                found = True
            elif found:
                mode, path = line[:2], line[3:]
                result[gitkeys.get(mode, 'modified')].append(path)
            else:
                result['mtime'] = max(result.get('mtime') or '', line.split()[0])
        return result
    
    async def pod_info(self, id, return_exceptions=False):
        if isinstance(id, list):
            return await asyncio.gather(*(self.pod_info(t) for t in id), return_exceptions=return_exceptions)
        nrec = await self._pod_info(id, return_exceptions=return_exceptions)
        if isinstance(nrec, Exception):
            return nrec
        name = nrec['name']
        metrics_url = f'namespaces/monitoring/services/heapster/proxy/apis/metrics/v1alpha1/namespaces/default/pods/{name}'
        if id.startswith('a2-'):
            resp2, resp3 = await self.get(metrics_url, ok404=True), None
        else:
            resp2, resp3 = await asyncio.gather(self.get(metrics_url, ok404=True), self._pod_changes(nrec))
        _pod_merge_metrics(nrec, resp2)
        if resp3 is not None:
            nrec['changes'] = resp3
        return nrec

    async def pod_log(self, id, container=None, follow=False, stream=None):
        data = await self._pod_info(id)
        if not container:
            container = 'editor' if id.startswith('a1-') else 'app'
        if container not in data['containers']:
            keys = ', '.join(sorted(data['containers'].keys()))
            raise KeyError(f'Container must be one of: {keys}')
        pname = data['name']
        cname = data['containers'][container]['name']
        if follow and (stream is None or isinstance(stream, io.TextIOWrapper)):
            stream = FileStream(stream)
        follow = str(bool(follow)).lower()
        path = f'namespaces/default/pods/{pname}/log?container={cname}&follow={follow}'
        ctype = 'text' if stream is None else 'content'
        result = await self.get(path, type=ctype)
        if ctype == 'text':
            return result
        await stream.prepare(result)
        async for data, eoc in result.content.iter_chunks():
            if stream.closing():
                await result.release()
                break
            await stream.write(data)
        await stream.finish()

    async def node_info(self):
        resp1 = self.get('nodes')
        resp2 = self.get('pods')
        resp3 = self.get('namespaces/monitoring/services/heapster/proxy/apis/metrics/v1alpha1/pods')
        resp1, resp2, resp3 = await asyncio.gather(resp1, resp2, resp3)
        resp1, resp2, resp3 = resp1['items'], resp2['items'], resp3['items']

        nodeMap = {}
        nodeList = []
        subsets = ('total', 'sessions', 'deployments', 'middleware', 'system')
        whiches = ('requests', 'limits', 'usage')
        for rec in resp1:
            nodeRec = {'name': rec['metadata']['name'],
                       'capacity': rec['status']['capacity'],
                       'allocatable': rec['status']['allocatable'],
                       'ready': any(c['type'] == 'Ready' and c['status'] == "True" for c in rec['status']['conditions']),
                       'conditions': [c['type'] for c in rec['status']['conditions']
                                      if c['type'] != 'Ready' and c['status'] == "True"],
                       'timestamp': None,
                       'window': None}
            for subset in subsets:
                srec = nodeRec[subset] = {'pods': 0, 'pending': 0}
                for which in whiches:
                    srec[which] = {'memory': 0, 'cpu': 0, 'nvidia.com/gpu': 0}
            nodeMap[nodeRec['name']] = nodeRec
            nodeList.append(nodeRec)
    
        podMap = {}
        for pod in resp2:
            nodeName = pod['spec']['nodeName']
            phase = pod['status']['phase']
            if phase in ('Failed', 'Succeeded') or nodeName not in nodeMap:
                continue
            pfld = 'pending' if phase == 'Pending' else 'pods'
            podName = pod['metadata']['name']
            if podName.startswith('anaconda-session'):
                t_sub = 'sessions'
            elif podName.startswith('anaconda-app'):
                t_sub = 'deployments'
            elif podName.startswith('anaconda-'):
                t_sub = 'middleware'
            else:
                t_sub = 'system'
            nodeRec = nodeMap[nodeName]
            podMap[podName] = [nodeRec, t_sub]
            for subset in ('total', t_sub):
                subRec = nodeRec[subset]
                subRec[pfld] += 1
                for container in pod['spec']['containers']:
                    for which in ('requests', 'limits'):
                        src = container['resources'].get(which, {})
                        dst = subRec[which]
                        for key, value in dst.items():
                            default = 'inf' if which == 'limits' and 'key' != 'nvidia.com/gpu' else '0'
                            dst[key] = value + _to_float(src.get(key, default))
                subRec['usage']['nvidia.com/gpu'] = subRec['requests']['nvidia.com/gpu']
                
        for pod in resp3:
            podName = pod['metadata']['name']
            if podName in podMap:
                nodeRec, t_sub = podMap[podName]
                if nodeRec['window'] is None:
                    nodeRec['window'] = pod['window']
                if nodeRec['timestamp'] is None:
                    nodeRec['timestamp'] = pod['timestamp']
                for subset in ('total', t_sub):
                    subRec = nodeRec[subset]
                    dst = subRec['usage']
                    for container in pod['containers']:
                        for key, value in container['usage'].items():
                            dst[key] += _to_float(value)

        for nodeRec in nodeList:
            for subset in subsets:
                for which in whiches:
                    dst = nodeRec[subset][which]
                    for key, value in dst.items():
                        dst[key] = _to_text(value)

        return nodeList
