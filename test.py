import redis
from confluent_kafka import Consumer, KafkaError

def test_connections():
    print("Diagnostic Starting")
    
    # 1. Test Redis (The Results Storage)
    try:
        r = redis.Redis(host='localhost', port=6379, socket_timeout=5)
        if r.ping():
            print("REDIS: Connected and Responding!")
    except Exception as e:
        print(f"REDIS: Failed. Is Docker Desktop running Redis? ({e})")

    # 2. Test Kafka (The Ingestion Feed)
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest',
        'socket.timeout.ms': 5000
    }
    
    try:
        consumer = Consumer(conf)
        # We ask for metadata to see if the broker exists
        metadata = consumer.list_topics(timeout=5)
        print(f"KAFKA: Connected! Found topics: {list(metadata.topics.keys())}")
        consumer.close()
    except Exception as e:
        print(f"KAFKA: Failed. Check Docker Desktop for Kafka/Zookeeper. ({e})")

if __name__ == "__main__":
    test_connections()