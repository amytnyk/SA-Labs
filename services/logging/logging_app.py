import os

import hazelcast
from fastapi import FastAPI
from pydantic import BaseModel


class LogMessage(BaseModel):
    id: str
    msg: str


app = FastAPI()
hazelcast_port = os.environ.get("HAZELCAST_PORT")

print(f"Using hazelcast on {hazelcast_port}")

client = hazelcast.HazelcastClient(
    cluster_name="cluster",
    cluster_members=[f"127.0.0.1:{hazelcast_port}"]
)
print("Connected")
log_storage = client.get_map("log").blocking()


@app.post("/log")
async def post_log(log: LogMessage):
    log_storage.put(log.id, log.msg)
    print(f"Message '{log.msg}' posted to {hazelcast_port}")
    return {"status": "ok"}


@app.get("/log")
async def get_logs():
    return list(log_storage.values())
