import json
import time
import paramiko
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'cisco-commands'
CONSUMER_GROUP = 'router-workers'

# Router credentials
ROUTER_USER = 'admin'
ROUTER_PASS = 'cisco'
ROUTER_PORT = 22

def execute_cisco_command(ip, username, password, command):
    print(f"\n[Worker] Connecting to Cisco Router {ip}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Cisco IOS requires an interactive shell session rather than exec_command
        ssh.connect(
            hostname=ip,
            port=ROUTER_PORT,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False,
            timeout=10
        )
        
        channel = ssh.invoke_shell()
        time.sleep(1)
        
        # Send terminal length 0 to prevent pagination pauses (--More--)
        channel.send("terminal length 0\n")
        time.sleep(0.5)
        
        # Send the actual command
        channel.send(f"{command}\n")
        time.sleep(2)  # Wait for output
        
        output = ""
        while channel.recv_ready():
            output += channel.recv(65535).decode('utf-8', errors='ignore')
            
        ssh.close()
        return output

    except Exception as e:
        return f"Error executing SSH command: {str(e)}"

def run_worker():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        group_id=CONSUMER_GROUP,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    print(f"[Worker] Started and listening on topic '{TOPIC_NAME}' (Group: {CONSUMER_GROUP})...")
    
    for message in consumer:
        task = message.value
        device_ip = task.get('device_ip')
        command = task.get('command')
        
        print(f"\n[Worker] Received task for Router ({device_ip}): '{command}'")
        
        # Execute via Paramiko
        result = execute_cisco_command(device_ip, ROUTER_USER, ROUTER_PASS, command)
        
        print("----- [Cisco Router Output] -----")
        print(result)
        print("---------------------------------")

if __name__ == '__main__':
    run_worker()
