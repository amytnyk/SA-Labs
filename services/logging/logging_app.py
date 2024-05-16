import os

import hazelcast
import uuid
from consul import Consul
from fastapi import FastAPI
from pydantic import BaseModel


class LogMessage(BaseModel):
    id: str
    msg: str


app = FastAPI()
port = os.environ.get("LOGGING_PORT")
consul_service = Consul()
consul_service.agent.service.register(name="logging-service", service_id=str(uuid.uuid4()), address="127.0.0.1",
                                      port=int(port))

print(f"Using port {port}")

client = hazelcast.HazelcastClient(
    cluster_name=consul_service.kv.get("hazelcast_cluster_name")[1]["Value"].decode("utf-8"),
    cluster_members=[consul_service.kv.get(f"hazelcast_log_address_{port}")[1]["Value"].decode("utf-8")]
)

log_storage = client.get_map("log").blocking()


@app.post("/log")
async def post_log(log: LogMessage):
    log_storage.put(log.id, log.msg)
    print(f"Message '{log.msg}' posted to {port}")
    return {"status": "ok"}


@app.get("/log")
async def get_logs():
    return list(log_storage.values())
