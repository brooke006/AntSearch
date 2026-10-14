"""Coordinator stub: a FastAPI service that records worker registrations in Redis."""

import logging
import os

import redis
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

log = logging.getLogger("antsearch.coordinator")

WORKERS_KEY = "antsearch:workers"


class Registration(BaseModel):
    worker_id: str


def create_app(redis_client: redis.Redis) -> FastAPI:
    app = FastAPI(title="AntSearch coordinator")

    @app.get("/health")
    def health():
        try:
            redis_client.ping()
        except redis.RedisError as exc:
            raise HTTPException(status_code=503, detail=f"redis unavailable: {exc}")
        return {"status": "ok"}

    @app.post("/workers/register")
    def register(registration: Registration):
        redis_client.sadd(WORKERS_KEY, registration.worker_id)
        log.info("registered worker %s", registration.worker_id)
        return {"worker_id": registration.worker_id, "registered": True}

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    uvicorn.run(
        create_app(client),
        host=os.environ.get("COORDINATOR_HOST", "127.0.0.1"),
        port=int(os.environ.get("COORDINATOR_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
