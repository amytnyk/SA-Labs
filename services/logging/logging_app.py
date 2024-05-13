from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class LogMessage(BaseModel):
    id: str
    msg: str


log_storage = {}


@app.post("/log")
async def post_log(log: LogMessage):
    log_storage[log.id] = log.msg
    return {"status": "ok"}


@app.get("/log")
async def get_logs():
    return list(log_storage.values())
