import json
import logging

import httpx

from antsearch.worker import register


def test_register_posts_worker_id_and_logs(caplog):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={})

    client = httpx.Client(base_url="http://coordinator", transport=httpx.MockTransport(handler))
    with caplog.at_level(logging.INFO, logger="antsearch.worker"):
        register(client, "w1")

    assert [r.url.path for r in requests] == ["/workers/register"]
    assert json.loads(requests[0].content) == {"worker_id": "w1"}
    assert "worker w1 registered" in caplog.text
