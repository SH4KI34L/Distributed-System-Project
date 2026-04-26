import json
import redis
import pandas as pd
from prophet import Prophet
from confluent_kafka import Consumer
from datetime import datetime

# 1. SETUP: Connectivity Configuration
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'loadlink-prediction-group',
    'auto.offset.reset': 'earliest'
}

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 2. DATA STORAGE: Temporary list to hold history for Prophet
# Prophet needs historical data to "learn" the trend
traffic_history = [] 
MIN_DATA_POINTS = 10 # Minimum points needed before we start predicting

def run_forecast(history):
    """
    Passes historical data to Facebook Prophet to predict the next value.
    """
    df = pd.DataFrame(history)
    # Prophet strictly requires columns named 'ds' (datestamp) and 'y' (value)
    df.columns = ['ds', 'y']
    df['ds'] = pd.to_datetime(df['ds'])

    # Initialize and fit the model
    # suppress_stdout_stderr is used here to keep your terminal clean
    model = Prophet(interval_width=0.95, daily_seasonality=True)
    model.fit(df)

    # Predict 1 step into the future
    future = model.make_future_dataframe(periods=1, freq='H')
    forecast = model.predict(future)
    
    predicted_val = forecast['yhat'].iloc[-1]
    return round(predicted_val, 2)

# 3. KAFKA: Initialize Consumer
consumer = Consumer(KAFKA_CONF)
topics = ['loadlink.traffic.raw'] # Focusing on traffic for Prophet
consumer.subscribe(topics)

print(f"--- Loadlink Forecasting Active: Waiting for {MIN_DATA_POINTS} data points ---")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None: continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            cafe_id = data.get('cafe_id', 'Unknown_Cafe')
            current_count = data['metrics']['current_traffic']
            
            # Prophet needs a timestamp. If ingestion doesn't send one, we create it.
            timestamp = data['metrics'].get('timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
            # Append to history: [Timestamp, Value]
            traffic_history.append({'ds': timestamp, 'y': current_count})
            
            # Keep the history manageable (e.g., last 100 points)
            if len(traffic_history) > 100:
                traffic_history.pop(0)

            print(f"Data Collected: {current_count} users at {timestamp}")

            # 4. PREDICTION LOGIC
            if len(traffic_history) >= MIN_DATA_POINTS:
                prediction = run_forecast(traffic_history)
                
                # Save prediction to Redis for the Frontend Lead
                redis_key = f"cafe:prediction:{cafe_id}"
                r.set(redis_key, prediction)
                
                print(f">>> PROPHET FORECAST for {cafe_id}: {prediction} expected users next hour.")

        except Exception as e:
            print(f"Processing Error: {e}")
            continue

except KeyboardInterrupt:
    print("\nStopping Prophet Processor...")
finally:
    consumer.close()