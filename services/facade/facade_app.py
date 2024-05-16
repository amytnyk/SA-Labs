import random

import hazelcast
from consul import Consul
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid
import requests


class Message(BaseModel):
    msg: str


app = FastAPI()

consul_service = Consul()
consul_service.agent.service.register(name="facade-service", service_id=str(uuid.uuid4()), address="127.0.0.1",
                                      port=8080)

client = hazelcast.HazelcastClient(
    cluster_name=consul_service.kv.get("hazelcast_cluster_name")[1]["Value"].decode("utf-8"),
    cluster_members=[consul_service.kv.get("hazelcast_queue_address")[1]["Value"].decode("utf-8")]
)

message_queue = client.get_queue(consul_service.kv.get("message_queue_name")[1]["Value"].decode("utf-8")).blocking()


def random_logging_service_url():
    return random.choice([
        f"http://{service['Address']}:{service['Port']}/log"
        for service in consul_service.agent.services().values()
        if service["Service"] == "logging-service"
    ])


def random_messages_service_url():
    return random.choice([
        f"http://{service['Address']}:{service['Port']}/message"
        for service in consul_service.agent.services().values()
        if service["Service"] == "messages-service"
    ])


@app.post("/facade")
async def post_message(message: Message):
    message_queue.offer(message.msg)
    response = requests.post(random_logging_service_url(), json={"id": str(uuid.uuid4()), "msg": message.msg})
    if not response.ok:
        raise HTTPException(status_code=response.status_code, detail=response.reason)

    return {"status": "ok"}


@app.get("/facade")
async def get_messages():
    logging_response = requests.get(random_logging_service_url())
    if not logging_response.ok:
        raise HTTPException(status_code=logging_response.status_code, detail=logging_response.reason)

    messages_response = requests.get(random_messages_service_url())
    if not messages_response.ok:
        raise HTTPException(status_code=messages_response.status_code, detail=messages_response.reason)

    return {"logs": logging_response.json(), "message": messages_response.json()}
