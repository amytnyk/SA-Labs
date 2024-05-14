from queue import Queue
from threading import Thread
from typing import Optional

import hazelcast
from fastapi import FastAPI

app = FastAPI()

client = hazelcast.HazelcastClient(
    cluster_name="cluster",
    cluster_members=["127.0.0.1:5704"]
)

message_queue = client.get_queue("message_queue").blocking()
messages = []
thread: Optional[Thread] = None


def run_consumer():
    while True:
        message = message_queue.take()
        print(f"Received message: {message}")
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
