from threading import Thread
from typing import Optional

import hazelcast
import uuid
import os
from consul import Consul
from fastapi import FastAPI

app = FastAPI()
port = os.environ.get("MESSAGES_PORT")
consul_service = Consul()
consul_service.agent.service.register(name="messages-service", service_id=str(uuid.uuid4()), address="127.0.0.1",
                                      port=int(port))

client = hazelcast.HazelcastClient(
    cluster_name=consul_service.kv.get("hazelcast_cluster_name")[1]["Value"].decode("utf-8"),
    cluster_members=[consul_service.kv.get("hazelcast_queue_address")[1]["Value"].decode("utf-8")]
)

message_queue = client.get_queue("message_queue").blocking()
messages = []
thread: Optional[Thread] = None


def run_consumer():
    while True:
        message = message_queue.take()
        print(f"Received message '{message}' on {port}")
        messages.append(message)


@app.on_event("startup")
async def startup():
    global thread
    thread = Thread(target=run_consumer, daemon=True)
    thread.start()


@app.on_event("shutdown")
async def shutdown():
    global thread
    thread.join(0)


@app.get("/message")
async def get_message():
    return messages
