import os
import json
import random
import time
import socket
from kafka import KafkaConsumer, KafkaProducer  # <-- Pure Python Drivers

KAFKA_URL = os.getenv("KAFKA_URL", "kafka-service:9092")
print("Consensus Engine Node spinning up...")

# --- FAIL PROOF SOLUTION: WAIT FOR PORT TO BE REACHABLE ---
def wait_for_kafka_network(host, port):
    print(f"Checking cluster network connectivity for {host}:{port}...")
    while True:
        try:

            time.sleep(120)
            with socket.create_connection((host, int(port)), timeout=3.0):
                print("Cluster network link successfully established! Proceeding...")
                return True
        except (socket.timeout, ConnectionRefusedError):
            print("Kafka service port is not reachable yet. Retrying in 3 seconds...")
            time.sleep(3)

# Parse your environment URL variable safely
kafka_host = KAFKA_URL.split(":")[0]
kafka_port = KAFKA_URL.split(":")[1]

# Holds execution until Kafka broker container is up
wait_for_kafka_network(kafka_host, kafka_port)

# Now it is completely safe to boot the consumers and producers without crashing
consumer = KafkaConsumer(
    "bids-topic",
    bootstrap_servers=[KAFKA_URL],
    group_id="consensus-engine-group",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_URL],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Consensus Engine fully online and actively polling for user offers!")

try:
    for message in consumer:
        data = message.value
        tx_id = data.get("tx_id")
        bid = data.get("bid_price")
        market = data.get("market_price")
        
        # Consensus negotiation matrix rules
        if bid >= market:
            verdict = "ACCEPT"
        elif (market - bid) / market < 0.15:
            verdict = random.choice(["ACCEPT", "DENY"])
        else:
            verdict = "DENY"
            
        print(f"Processed transaction [{tx_id}]. Verdict: {verdict}")
        
        response = {"tx_id": tx_id, "status": verdict}
        producer.send("verdicts-topic", response)
        producer.flush()

except Exception as main_err:
    print(f"Runtime execution failure: {main_err}")
