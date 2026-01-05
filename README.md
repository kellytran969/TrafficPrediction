# Traffic Prediction
Predict traffic in NYC
This project collects and analyzes real-time traffic data from NYC DOT sensors and lays the foundation for a traffic prediction system.

## Features
- Fetches real-time traffic speed data from NYC Open Data (no API key needed)
- Parses and structures data including speed, travel time, status, and location
- Computes statistics and identifies congested areas (<15 mph)
- Saves data for downstream ML modeling or time-series analysis

## Technologies
- Python, Requests, JSON, Pandas, Matplotlib/Seaborn
- Version control with Git and GitHub

## Usage
```bash
python scripts/traffic_ingestion.py