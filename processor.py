import json
import redis
import pandas as pd
from prophet import Prophet
from confluent_kafka import Consumer
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
import time

# Silence background Prophet logs
logging.getLogger('cmdstanpy').setLevel(logging.ERROR)

# --- CONFIGURATION (UPDATE THESE) ---
INFLUX_CONF = {
    "url": "http://localhost:8086",
    "token": "XjEoEBACxG15ZIkgrr3QAbHTkXjXb8bYUq3mjNpCNjzUYkV8u4rurEowA3tpeCWKAQLNnRPPGfBtx2VdQ15tIQ==", # Get from InfluxDB UI
    "org": "Loadlink",
    "bucket": "loadlink_traffic"
}

# New Group ID to ensure we start reading fresh data
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092', 
    'group.id': 'loadlink-final-demo-group-01',
    'auto.offset.reset': 'earliest'
}

# --- CLIENT INITIALIZATION ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
influx_client = InfluxDBClient(url=INFLUX_CONF["url"], token=INFLUX_CONF["token"], org=INFLUX_CONF["org"])
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

branch_histories = {}
MIN_DATA_POINTS = 10

def run_forecast(history):
    """
    Fits a Prophet model to the branch history and predicts the next hour.
    """
    df = pd.DataFrame(history)
    df['ds'] = pd.to_datetime(df['ds'])
    
    # Normalize timeline: 10 mins apart per data point for model stability
    start_time = datetime.now()
    df['ds'] = [start_time + timedelta(minutes=10*i) for i in range(len(df))]
    
    # Flat growth is best for stable, realistic café predictions
    model = Prophet(
        growth='flat', 
        daily_seasonality=False, 
        weekly_seasonality=False, 
        yearly_seasonality=False
    )
    model.fit(df)
    
    future = model.make_future_dataframe(periods=1, freq='h')
    forecast = model.predict(future)
    return int(max(0, forecast['yhat'].iloc[-1]))

# --- MAIN CONSUMER LOOP ---
consumer = Consumer(KAFKA_CONF)
consumer.subscribe(['loadlink.traffic.raw'])

print(f"--- Loadlink System: AI + InfluxDB Persistence Active ---")

try:
    while True:
        msg = consumer.poll(1.0) # Listen for 1 second
        if msg is None: continue
        if msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue

        try:
            # Decode message
            data = json.loads(msg.value().decode('utf-8'))
            b_id = data.get('branch_id', 'Unknown')
            count = data['metrics']['current_traffic']
            
            # 1. Manage history per branch to avoid data leakage
            if b_id not in branch_histories:
                branch_histories[b_id] = []
            
            branch_histories[b_id].append({'ds': datetime.now(), 'y': count})
            
            # Keep a sliding window of the last 20 points
            if len(branch_histories[b_id]) > 20:
                branch_histories[b_id].pop(0)

            # 2. Run Prediction if we have enough data
            prediction = 0
            if len(branch_histories[b_id]) >= MIN_DATA_POINTS:
                prediction = run_forecast(branch_histories[b_id])
                # Save to Redis for real-time dashboarding
                r.set(f"cafe:prediction:{b_id}", prediction)

            # 3. Save to InfluxDB for historical auditing
            point = Point("traffic_stats") \
                .tag("branch", b_id) \
                .field("actual", float(count)) \
                .field("forecast", float(prediction)) \
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            
            write_api.write(bucket=INFLUX_CONF["bucket"], org=INFLUX_CONF["org"], record=point)
            
            # Success Log
            status = "🔮 Predicting..." if prediction > 0 else "⏳ Collecting..."
            print(f"[{b_id}] Actual: {count} | Forecast: {prediction} | {status}")

        except Exception as e:
            print(f"Loop Error: {e}")
            continue

except KeyboardInterrupt:
    print("\nShutting down Loadlink Processor...")
finally:
    consumer.close()