import json
import redis
from confluent_kafka import Consumer
from transformers import pipeline

# 1. SETUP: Connectivity Configuration
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cafelink-processors',
    'auto.offset.reset': 'earliest'
}

# Connect to Redis (Alpine container)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 2. THE BRAIN: Load Sentiment Model
print("--- Loading AI Models (DistilBERT) ---")
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

# 3. KAFKA: Initialize Consumer and Subscribe to your Hybrid Topics
consumer = Consumer(KAFKA_CONF)
topics = ['cafe.traffic.raw', 'cafe.feedback.raw']
consumer.subscribe(topics)

print(f"--- CafeLink Processor Active: Listening on {topics} ---")

try:
    while True:
        msg = consumer.poll(1.0) # Wait 1 second for new data
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue

        # Parse the JSON from the Ingestion Lead
        try:
            data = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()
            cafe_id = data.get('cafe_id', 'Unknown_Cafe')
        except Exception as e:
            print(f"Data Format Error: {e}")
            continue

        # 4. LOGIC: Process based on the specific topic
        if topic == 'cafe.feedback.raw':
            feedback_text = data['payload']['text']
            
            # Run AI Analysis
            analysis = sentiment_analyzer(feedback_text)[0]
            sentiment = analysis['label'] # POSITIVE or NEGATIVE
            score = round(analysis['score'], 4)

            # SAVE TO REDIS: Key = cafe:sentiment:[ID], Value = the result
            redis_key = f"cafe:sentiment:{cafe_id}"
            r.set(redis_key, json.dumps({"sentiment": sentiment, "confidence": score}))
            
            print(f"Processed Feedback for {cafe_id}: {sentiment} ({score})")

        elif topic == 'cafe.traffic.raw':
            current_users = data['metrics']['current_traffic']
            
            # SAVE TO REDIS: Key = cafe:traffic:[ID], Value = current count
            redis_key = f"cafe:traffic:{cafe_id}"
            r.set(redis_key, current_users)
            
            print(f"Updated Traffic for {cafe_id}: {current_users} users")

except KeyboardInterrupt:
    print("\nStopping Processor...")
finally:
    consumer.close()