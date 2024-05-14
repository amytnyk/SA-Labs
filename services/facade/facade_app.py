import random

import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid
import requests

app = FastAPI()


class Message(BaseModel):
    msg: str


logging_service_urls = ["http://localhost:8011/log", "http://localhost:8012/log", "http://localhost:8013/log"]
messages_service_urls = ["http://localhost:8021/message", "http://localhost:8022/message"]

client = hazelcast.HazelcastClient(
    cluster_name="cluster",
    cluster_members=["127.0.0.1:5704"]
)

message_queue = client.get_queue("message_queue").blocking()


def random_logging_service_url():
    return random.choice(logging_service_urls)


def random_messages_service_url():
    return random.choice(messages_service_urls)


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
