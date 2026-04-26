# Loadlink: Distributed AI Traffic Processor
**Role:** Processing Worker Lead 
**Status:** Environment Configured ,Integration Ready & AI worker is sitting idle

## Overview
This service acts as the "Brain" of the Loadlink system. It consumes real-time traffic data from Kafka, processes metrics with Pandas, and generates 1-hour forecasts using Meta's Prophet model.

## How It Works (The Logic)
1. **Listen:** The worker polls the Kafka topic for new traffic counts.
2. **Transform:** Raw JSON data is converted into a Time-Series dataframe.
3. **Analyze:** Once 10 data points are gathered, the Prophet model fits the data.
4. **Predict:** A forecast is generated and pushed to Redis for instant retrieval by the Frontend.

## Setup Instructions
### 1. Environment
1. **Python Version:** 3.10 or higher
2. **Virtual Env:** Active `(venv)` required
3. **Dependencies:** `prophet`, `pandas`, `confluent-kafka`, `redis`

### 2.Connectivity
1. **Kafka Broker:** `127.0.0.1:9092`
2. **Redis Host:** `127.0.0.1:6379`
3. **Topic:** `loadlink.traffic.raw`

### 3.Running the Service
```powershell
.\venv\Scripts\activate
python processor.py