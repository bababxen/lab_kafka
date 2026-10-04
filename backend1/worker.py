import json
import time
import paramiko
from confluent_kafka import Consumer, KafkaError

KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cisco-worker-group',
    'auto.offset.reset': 'earliest'
}

ROUTER_IP = '192.168.39.130'
ROUTER_USER = 'admin'
ROUTER_PASS = 'cisco'

def run_ssh_command(ip, command):
    print(f"\n[Worker] Connecting to Router {ip} via SSH...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(
            hostname=ip,
            port=22,
            username=ROUTER_USER,
            password=ROUTER_PASS,
            look_for_keys=False,
            allow_agent=False,
            timeout=10
        )
        
        channel = ssh.invoke_shell()
        time.sleep(1)
        channel.send("terminal length 0\n")
        time.sleep(0.5)
        
        channel.send(f"{command}\n")
        time.sleep(2)
        
        output = ""
        while channel.recv_ready():
            output += channel.recv(65535).decode('utf-8', errors='ignore')
            
        ssh.close()
        return output
    except Exception as e:
        return f"SSH Error: {e}"

def main():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(['cisco.commands'])
    print("[Worker] Waiting for Cisco commands from Kafka topic 'cisco.commands'...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            payload_raw = msg.value().decode('utf-8')
            print(f"\n[Worker] Received message from Kafka: {payload_raw}")
            
            try:
                data = json.loads(payload_raw)
                device_ip = data.get('device_ip', ROUTER_IP)
                cmd = data.get('command')
                
                if cmd:
                    output = run_ssh_command(device_ip, cmd)
                    print("\n========== Cisco Router Output ==========")
                    print(output)
                    print("=========================================\n")
            except json.JSONDecodeError:
                print(f"[Worker] Skip non-JSON message: {payload_raw}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
