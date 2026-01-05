
import requests
import json
from datetime import datetime
import time

class TrafficDataIngestion:
    """
    Traffic data ingestion from NYC Open Data - Real-Time Traffic Speed Data
    100% free, no API key needed, real sensor data from NYC highways!
    """
    
    def __init__(self):
        # NYC Open Data - Real-Time Traffic Speed Data
        self.base_url = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
        
    def fetch_traffic_data(self, limit=1000):
        """
        Fetch real-time traffic speed data from NYC DOT sensors
        
        Args:
            limit: Number of records to fetch (default 1000)
        """
        try:
            params = {
                '$limit': limit,
                '$order': 'data_as_of DESC'
            }
            response = requests.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
    
    def parse_traffic_data(self, data):
        """
        Parse and extract relevant traffic metrics from NYC data
        """
        if not data:
            return []
        
        parsed_results = []
        for record in data:
            traffic_record = {
                'timestamp': record.get('data_as_of', datetime.utcnow().isoformat()),
                'link_id': record.get('id', 'Unknown'),
                'link_name': record.get('link_name', 'Unknown'),
                'borough': record.get('borough', 'Unknown'),
                'speed': float(record.get('speed', 0)),
                'travel_time': float(record.get('travel_time', 0)),
                'status': record.get('status', 'Unknown'),
                'data_as_of': record.get('data_as_of', 'Unknown'),
                'link_points': record.get('link_points', 'Unknown'),
                'encoded_poly_line': record.get('encoded_poly_line', 'Unknown'),
                'owner': record.get('owner', 'Unknown')
            }
            parsed_results.append(traffic_record)
        
        return parsed_results
    
    def get_statistics(self, parsed_data):
        """
        Calculate statistics from traffic data
        """
        if not parsed_data:
            return {}
        
        speeds = [r['speed'] for r in parsed_data if r['speed'] > 0]
        travel_times = [r['travel_time'] for r in parsed_data if r['travel_time'] > 0]
        
        # Count by borough
        borough_counts = {}
        for record in parsed_data:
            borough = record['borough']
            borough_counts[borough] = borough_counts.get(borough, 0) + 1
        
        # Count by status
        status_counts = {}
        for record in parsed_data:
            status = record['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        stats = {
            'total_segments': len(parsed_data),
            'avg_speed': sum(speeds) / len(speeds) if speeds else 0,
            'min_speed': min(speeds) if speeds else 0,
            'max_speed': max(speeds) if speeds else 0,
            'avg_travel_time': sum(travel_times) / len(travel_times) if travel_times else 0,
            'borough_counts': borough_counts,
            'status_counts': status_counts
        }
        
        return stats
    
    def print_traffic_summary(self, parsed_data):
        """
        Print a summary of traffic data
        """
        print(f"\n{'='*80}")
        print(f"NYC Real-Time Traffic Data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        if not parsed_data:
            print("No traffic data available")
            return
        
        stats = self.get_statistics(parsed_data)
        
        print(f"📊 OVERALL STATISTICS")
        print(f"{'─'*80}")
        print(f"Total road segments: {stats['total_segments']}")
        print(f"Average speed: {stats['avg_speed']:.1f} mph")
        print(f"Speed range: {stats['min_speed']:.1f} - {stats['max_speed']:.1f} mph")
        print(f"Average travel time: {stats['avg_travel_time']:.1f} seconds")
        print()
        
        print(f"🗽 BY BOROUGH")
        print(f"{'─'*80}")
        for borough, count in sorted(stats['borough_counts'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {borough}: {count} segments")
        print()
        
        print(f"🚦 TRAFFIC STATUS")
        print(f"{'─'*80}")
        for status, count in sorted(stats['status_counts'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {status}: {count} segments")
        print()
        
        print(f"🛣️  SAMPLE ROAD SEGMENTS (First 10)")
        print(f"{'─'*80}")
        for idx, record in enumerate(parsed_data[:10], 1):
            print(f"\nSegment {idx}:")
            print(f"  Link ID: {record['link_id']}")
            print(f"  Name: {record['link_name']}")
            print(f"  Borough: {record['borough']}")
            print(f"  Current Speed: {record['speed']:.1f} mph")
            print(f"  Travel Time: {record['travel_time']:.1f} seconds")
            print(f"  Status: {record['status']}")
            print(f"  Data as of: {record['data_as_of']}")
        
        if len(parsed_data) > 10:
            print(f"\n... and {len(parsed_data) - 10} more segments")
    
    def find_congested_areas(self, parsed_data, speed_threshold=15):
        """
        Find areas with heavy congestion (speed below threshold)
        """
        congested = [r for r in parsed_data if r['speed'] > 0 and r['speed'] < speed_threshold]
        congested.sort(key=lambda x: x['speed'])
        
        print(f"\n{'='*80}")
        print(f"🚨 CONGESTED AREAS (Speed < {speed_threshold} mph)")
        print(f"{'='*80}\n")
        
        if not congested:
            print("No significant congestion detected!")
            return
        
        print(f"Found {len(congested)} congested segments\n")
        
        for idx, record in enumerate(congested[:15], 1):
            print(f"{idx}. {record['link_name']} ({record['borough']})")
            print(f"   Speed: {record['speed']:.1f} mph | Status: {record['status']}")
            print()


# Example usage
if __name__ == "__main__":
    print("="*80)
    print("NYC REAL-TIME TRAFFIC DATA INGESTION - 100% FREE!")
    print("="*80)
    print("\nFetching real-time traffic data from NYC DOT sensors...")
    print("No API key needed! Data source: NYC Open Data\n")
    
    ingestion = TrafficDataIngestion()
    
    # Fetch traffic data
    print("Fetching data...")
    raw_data = ingestion.fetch_traffic_data(limit=1000)
    
    if raw_data:
        print(f"✓ Successfully fetched {len(raw_data)} traffic segments\n")
        
        # Parse the data
        parsed_data = ingestion.parse_traffic_data(raw_data)
        
        # Print summary
        ingestion.print_traffic_summary(parsed_data)
        
        # Find congested areas
        ingestion.find_congested_areas(parsed_data, speed_threshold=15)
        
        # Save to file
        output_file = f"nyc_traffic_data_{int(time.time())}.json"
        with open(output_file, 'w') as f:
            json.dump(parsed_data, f, indent=2)
        print(f"\n{'='*80}")
        print(f"💾 Data saved to {output_file}")
        print(f"{'='*80}")
    else:
        print("❌ Failed to fetch traffic data")
    
    print("\n" + "="*80)
    print("NEXT STEPS:")
    print("="*80)
    print("1. Run this script every 5-10 minutes to build a time-series dataset")
    print("2. Store in a database (PostgreSQL, TimescaleDB, InfluxDB)")
    print("3. Analyze patterns: rush hour, weekday vs weekend, weather impact")
    print("4. Build ML models to predict traffic based on time/day/conditions")
    print("5. Create a streaming pipeline (Kafka/Kinesis) for real-time processing")
    print("="*80)