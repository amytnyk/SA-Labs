from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid
import requests

app = FastAPI()


class Message(BaseModel):
    msg: str


logging_service_url = "http://localhost:8001/log"
messages_service_url = "http://localhost:8002/message"


@app.post("/facade")
async def post_message(message: Message):
    response = requests.post(logging_service_url, json={"id": str(uuid.uuid4()), "msg": message.msg})
    if not response.ok:
        raise HTTPException(status_code=response.status_code, detail=response.reason)

    return {"status": "ok"}


@app.get("/facade")
async def get_messages():
    logging_response = requests.get(logging_service_url)
    if not logging_response.ok:
        raise HTTPException(status_code=logging_response.status_code, detail=logging_response.reason)

    messages_response = requests.get(messages_service_url)
    if not messages_response.ok:
        raise HTTPException(status_code=messages_response.status_code, detail=messages_response.reason)

    return {"logs": logging_response.json(), "message": messages_response.json()}
