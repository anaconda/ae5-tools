import os

DEFAULT_K8S_URL = "https://kubernetes.default/"
DEFAULT_K8S_TOKEN_FILES = (
    "/var/run/secrets/kubernetes.io/serviceaccount/token",
    "/var/run/secrets/user_credentials/k8s_token",
)
K8S_ENDPOINT_PORT = int(os.environ.get("AE5_K8S_PORT") or "8086")
DEFAULT_PROMETHEUS_PORT = 9090
