import fakeredis
from fastapi.testclient import TestClient

from antsearch.coordinator import WORKERS_KEY, create_app


def test_register_adds_worker_to_redis():
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    client = TestClient(create_app(redis_client))

    response = client.post("/workers/register", json={"worker_id": "w1"})

    assert response.status_code == 200
    assert redis_client.smembers(WORKERS_KEY) == {"w1"}
