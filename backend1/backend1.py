import json
import time
from kafka import KafkaProducer

# If running on Ubuntu VM where Kafka container is mapped to port 9092:
# use 'localhost:9092' (or '192.168.219.129:9092' if calling from host Windows)
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'cisco-commands'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_command(device_ip, command):
    payload = {
        'device_ip': device_ip,
        'command': command,
        'timestamp': time.time()
    }
    
    print(f"[Backend1] Sending task: {payload} to topic '{TOPIC_NAME}'...")
    future = producer.send(TOPIC_NAME, value=payload)
    
    # Block for 'synchronous' sends to ensure message delivery
    record_metadata = future.get(timeout=10)
    print(f"[Backend1] Message delivered to {record_metadata.topic} [partition: {record_metadata.partition}]")

if __name__ == '__main__':
    # Generate and send the command specified in your diagram
    target_router_ip = "192.168.39.130"
    cli_command = "show ip interface brief"
    
    send_command(target_router_ip, cli_command)
    producer.flush()
    producer.close()
