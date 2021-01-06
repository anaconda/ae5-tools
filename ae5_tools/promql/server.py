import os
import sys
import json
import datetime
import requests
from flask import Flask, request, Response

DEFAULT_K8S_URL = 'https://10.100.0.1/'
DEFAULT_K8S_TOKEN_FILE = '/var/run/secrets/user_credentials/k8s_token'
PROMETHEUS_PORT = 9090
VERIFY_FLAG = False # FIXME

class AE5PromQLHandler(object):
    def __init__(self, promql_IP):
        self._promql_IP = promql_IP

    @classmethod
    def _get_promQL_IP(cls):
        "Get the IP for the prometheus-k8s service"
        resp = k8s.get(DEFAULT_K8S_URL + 'api/v1/namespaces/monitoring/services/')
        entries = [el for el in resp.json()['items']
                   if el['metadata']['name'] == 'prometheus-k8s']
        assert len(entries)==1, "More than one prometheus-k8s service found"
        return  entries[0]['spec']['clusterIP']

    @app.route('/query_range', methods=['GET', 'POST'])
    def query_range(self):
        PFX = f'http://{self._promql_IP}:{PROMETHEUS_PORT}/'
        if request.method == 'GET':
            now = datetime.datetime.utcnow()
            end_timestamp = now.isoformat("T") + "Z"
            start_limit = now - datetime.timedelta(weeks=52*10)
            start_timestamp = now.isoformat("T") + "Z"
            try:
                query = request.args.get('query', None)
                metric = request.args.get('metric', None) # e.g container_cpu_usage_seconds_total
                start = request.args.get('start', start_timestamp)
                end = request.args.get('end', end_timestamp)

                pod_id = request.args.get('id', None)
                step = request.args.get('step', '100s')
                if query is None:
                    regex = f'anaconda-app-{pod_id}-.*'
                    query = f"{metric}{{container_name='app',pod_name=~'{regex}'}}"
                url = PFX + f'api/v1/query_range?query={query}&start={start}&end={end}&step={step}'
                resp = k8s.get(url,  verify=VERIFY_FLAG)
                if resp.status_code == 400:
                    print(resp.text)
                if resp.status_code != 200:
                    return Response('{}', resp.status_code, content_type='application/json')
                json_result = resp.json()
                return Response(json.dumps(json_result['data']['result'][0]['values'],
                                           indent=2), content_type='application/json')
            except Exception as e:
                print("Failure to fetch JSON: %s" % str(e))
        return Response('{}', 200, content_type='application/json')


def main(url=None, token=None, port=None):
    url = url or os.environ.get('AE5_K8S_URL', DEFAULT_K8S_URL)
    if token is None:
        token = os.environ.get('AE5_K8S_TOKEN')
    if token is None:
        token_file = os.environ.get('AE5_K8S_TOKEN_FILE', DEFAULT_K8S_TOKEN_FILE)
        if token_file and os.path.exists(token_file):
            with open(token_file, 'r') as fp:
                token = fp.read().strip()

    app = Flask(__name__)
    k8s = requests.Session()
    k8s.verify = VERIFY_FLAG
    k8s.headers['Authorization'] = f'Bearer {token}'
    port = port or int(os.environ.get('AE5_PROMQL_PORT') or '8086')
    AE5PromQLHandler(AE5PromQLHandler._get_promQL_IP())
    # PromQL deployment should have its own port number
    app.run(host='0.0.0.0', port=8086, debug=True)

if __name__ == '__main__':
    url = sys.argv[1] if len(sys.argv) > 1 else None
    main(url=url, token=False if url else None)