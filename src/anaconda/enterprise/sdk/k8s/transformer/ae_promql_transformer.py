import datetime

from .ae_base_transformer import AEBaseTransformer
from .utils import parse_timedelta


class AEPromQLTransformer(AEBaseTransformer):
    async def query_range(
        self, pod_id=None, query=None, metric=None, start=None, end=None, step=None, period=None, samples=None
    ):
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
