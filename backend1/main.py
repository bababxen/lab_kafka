from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import json

app = FastAPI()

conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(conf)

class CommandRequest(BaseModel):
    device_ip: str = "192.168.39.130"
    command: str = "show ip interface brief"

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

@app.post("/send-command")
def send_cisco_command(payload: CommandRequest):
    try:
        data = {
            "device_ip": payload.device_ip,
            "command": payload.command
        }
        producer.produce(
            topic='cisco.commands',
            value=json.dumps(data).encode('utf-8'),
            callback=delivery_report
        )
        producer.flush()
        return {"status": "success", "sent_data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
