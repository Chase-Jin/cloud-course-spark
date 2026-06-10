import os
import time
from typing import Dict, Any

from flask import Flask, jsonify, request
import redis

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "") or None


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=2,
    )


def redis_status() -> Dict[str, Any]:
    try:
        client = get_redis_client()
        client.ping()
        return {"redis": "ok", "redis_host": REDIS_HOST, "redis_port": REDIS_PORT}
    except Exception as exc:  # 保证 /api/ping 在 Redis 短暂异常时仍返回 status=ok，便于 HPA 压测
        return {"redis": "error", "error": str(exc), "redis_host": REDIS_HOST, "redis_port": REDIS_PORT}


@app.before_request
def log_request() -> None:
    app.logger.info("%s %s from %s", request.method, request.path, request.remote_addr)


@app.get("/api/ping")
def ping():
    payload = {
        "status": "ok",
        "service": "flask-backend",
        "time": int(time.time()),
    }
    payload.update(redis_status())
    return jsonify(payload)


@app.post("/api/kv/<key>")
def set_key(key: str):
    value = request.json.get("value") if request.is_json else request.form.get("value")
    if value is None:
        return jsonify({"status": "error", "message": "missing value"}), 400
    client = get_redis_client()
    client.set(key, value)
    return jsonify({"status": "ok", "key": key, "value": value})


@app.get("/api/kv/<key>")
def get_key(key: str):
    client = get_redis_client()
    value = client.get(key)
    return jsonify({"status": "ok", "key": key, "value": value})


@app.get("/api/count")
def count():
    client = get_redis_client()
    current = client.incr("visit_count")
    return jsonify({"status": "ok", "visit_count": current})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
