import json
import time
import random
from kafka import KafkaProducer

# Initialize the Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'loadlink.traffic.raw'
BRANCHES = ['Branch_A', 'Branch_B']

print("--- [MOCK INGESTION] Starting Clean Time-Series Stream ---")

try:
    while True:
        for branch in BRANCHES:
            payload = {
                "branch_id": branch,
                "timestamp": time.time(),
                "metrics": {
                    "current_traffic": random.randint(15, 45) 
                }
            }
            # Send and FORCE the push to Kafka
            producer.send(TOPIC, payload)
            producer.flush() 
            
            print(f"📡 Sent to {branch}: {payload['metrics']['current_traffic']} users")
            time.sleep(1) 
        
        # Wait 10 seconds before the next round of data
        time.sleep(10)
except KeyboardInterrupt:
    print("\nStopping Mock Data...")