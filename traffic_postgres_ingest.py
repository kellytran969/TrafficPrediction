import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import time
import json

class TrafficDataPipeline:
    """
    Complete traffic data pipeline with PostgreSQL:
    1. Fetch from NYC Open Data
    2. Store in PostgreSQL
    3. Provide analytics
    """
    
    def __init__(self, db_config):
        """
        Initialize with PostgreSQL connection config
        
        Args:
            db_config: dict with keys: host, port, database, user, password
        """
        self.api_url = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
        self.db_config = db_config
        self.connection = None
        
    def connect_db(self):
        """Establish connection to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            print("✓ Connected to PostgreSQL")
            return True
        except Exception as error:
            print(f"❌ Database connection error: {error}")
            return False
    
    def disconnect_db(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("✓ Database connection closed")
    
    def fetch_traffic_data(self, limit=1000):
        """Fetch real-time traffic data from NYC Open Data"""
        try:
            params = {
                '$limit': limit,
                '$order': 'data_as_of DESC'
            }
            response = requests.get(self.api_url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ API error: {e}")
            return None
    
    def insert_traffic_data(self, records):
        """
        Insert traffic records into PostgreSQL
        Uses ON CONFLICT to handle duplicates
        """
        if not self.connection:
            print("❌ No database connection")
            return 0
        
        cursor = self.connection.cursor()
        
        # SQL INSERT with ON CONFLICT (upsert)
        insert_sql = """
            INSERT INTO traffic_data 
                (time_stamp, link_id, link_name, borough, speed, 
                 travel_time, status, data_as_of, owner)
            VALUES %s
            ON CONFLICT (link_id, time_stamp) 
            DO UPDATE SET
                speed = EXCLUDED.speed,
                travel_time = EXCLUDED.travel_time,
                status = EXCLUDED.status
        """
        
        # Prepare batch data
        batch_data = []
        for record in records:
            # Parse timestamp
            try:
                ts_str = record.get('data_as_of', '')
                if ts_str:
                    # Handle ISO format with 'T' and 'Z'
                    ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                else:
                    ts = datetime.utcnow()
            except:
                ts = datetime.utcnow()
            
            batch_data.append((
                ts,
                record.get('id', 'Unknown'),
                record.get('link_name', 'Unknown'),
                record.get('borough', 'Unknown'),
                float(record.get('speed', 0)),
                float(record.get('travel_time', 0)),
                record.get('status', 'Unknown'),
                ts,
                record.get('owner', 'Unknown')
            ))
        
        try:
            # Execute batch insert using execute_values (much faster)
            execute_values(cursor, insert_sql, batch_data)
            self.connection.commit()
            inserted = cursor.rowcount
            cursor.close()
            print(f"✓ Inserted/Updated {inserted} records")
            return inserted
        except Exception as error:
            print(f"❌ Insert error: {error}")
            self.connection.rollback()
            cursor.close()
            return 0
    
    def get_statistics(self):
        """Get database statistics"""
        if not self.connection:
            return None
        
        cursor = self.connection.cursor()
        
        stats = {}
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM traffic_data")
        stats['total_records'] = cursor.fetchone()[0]
        
        # Date range
        cursor.execute("""
            SELECT MIN(time_stamp), MAX(time_stamp) 
            FROM traffic_data
        """)
        date_range = cursor.fetchone()
        stats['earliest_data'] = date_range[0]
        stats['latest_data'] = date_range[1]
        
        # Unique links
        cursor.execute("SELECT COUNT(DISTINCT link_id) FROM traffic_data")
        stats['unique_links'] = cursor.fetchone()[0]
        
        # Records by borough
        cursor.execute("""
            SELECT borough, COUNT(*) as count
            FROM traffic_data
            GROUP BY borough
            ORDER BY count DESC
        """)
        stats['by_borough'] = cursor.fetchall()
        
        # Average speed by borough (recent data - last hour)
        cursor.execute("""
            SELECT borough, ROUND(AVG(speed)::numeric, 2) as avg_speed
            FROM traffic_data
            WHERE time_stamp >= NOW() - INTERVAL '1 hour'
            AND speed > 0
            GROUP BY borough
            ORDER BY avg_speed
        """)
        stats['avg_speed_by_borough'] = cursor.fetchall()
        
        # Most congested links (last hour)
        cursor.execute("""
            SELECT link_name, borough, ROUND(AVG(speed)::numeric, 2) as avg_speed
            FROM traffic_data
            WHERE time_stamp >= NOW() - INTERVAL '1 hour'
            AND speed > 0
            GROUP BY link_name, borough
            ORDER BY avg_speed
            LIMIT 5
        """)
        stats['most_congested'] = cursor.fetchall()
        
        cursor.close()
        return stats
    
    def print_statistics(self, stats):
        """Print database statistics"""
        print(f"\n{'='*80}")
        print(f"DATABASE STATISTICS")
        print(f"{'='*80}\n")
        
        print(f"📊 OVERALL")
        print(f"  Total records: {stats['total_records']:,}")
        print(f"  Unique road links: {stats['unique_links']:,}")
        if stats['earliest_data'] and stats['latest_data']:
            duration = stats['latest_data'] - stats['earliest_data']
            print(f"  Data range: {stats['earliest_data']} to {stats['latest_data']}")
            print(f"  Duration: {duration}")
        print()
        
        if stats['by_borough']:
            print(f"🗽 RECORDS BY BOROUGH")
            for borough, count in stats['by_borough']:
                print(f"  {borough}: {count:,}")
            print()
        
        if stats['avg_speed_by_borough']:
            print(f"🚗 AVERAGE SPEED (Last Hour)")
            for borough, avg_speed in stats['avg_speed_by_borough']:
                print(f"  {borough}: {avg_speed} mph")
            print()
        
        if stats['most_congested']:
            print(f"🚨 MOST CONGESTED LINKS (Last Hour)")
            for idx, (link_name, borough, avg_speed) in enumerate(stats['most_congested'], 1):
                print(f"  {idx}. {link_name} ({borough}): {avg_speed} mph")
            print()
    
    def run_collection_cycle(self):
        """Run one complete data collection cycle"""
        print(f"\n{'='*80}")
        print(f"COLLECTION CYCLE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        # Fetch data
        print("1. Fetching traffic data from NYC Open Data...")
        raw_data = self.fetch_traffic_data(limit=1000)
        
        if not raw_data:
            print("❌ Failed to fetch data")
            return False
        
        print(f"✓ Fetched {len(raw_data)} records")
        
        # Connect to DB
        print("\n2. Connecting to PostgreSQL...")
        if not self.connect_db():
            return False
        
        # Insert data
        print("\n3. Inserting data into database...")
        inserted = self.insert_traffic_data(raw_data)
        
        # Get statistics
        print("\n4. Retrieving database statistics...")
        stats = self.get_statistics()
        if stats:
            self.print_statistics(stats)
        
        # Disconnect
        self.disconnect_db()
        
        print(f"{'='*80}")
        print(f"CYCLE COMPLETE ✓")
        print(f"{'='*80}\n")
        
        return True


def continuous_collection(interval_minutes=5):
    """
    Run continuous data collection
    
    Args:
        interval_minutes: Time between collection cycles
    """
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'traffic_db',
        'user': 'traffic_user',
        'password': 'traffic123'
    }
    
    pipeline = TrafficDataPipeline(db_config)
    
    print("="*80)
    print("TRAFFIC DATA COLLECTION PIPELINE - PostgreSQL")
    print("="*80)
    print(f"Collection interval: {interval_minutes} minutes")
    print(f"Target: PostgreSQL at {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print(f"Press Ctrl+C to stop")
    print("="*80)
    
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            print(f"\n\n>>> CYCLE {cycle_count} <<<")
            
            success = pipeline.run_collection_cycle()
            
            if success:
                next_run = datetime.now() + timedelta(minutes=interval_minutes)
                print(f"⏰ Next collection at: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Sleeping for {interval_minutes} minutes...")
                time.sleep(interval_minutes * 60)
            else:
                print("⚠️  Error in cycle, retrying in 1 minute...")
                time.sleep(60)
                
    except KeyboardInterrupt:
        print("\n\n✓ Stopped by user")
        print(f"Total cycles completed: {cycle_count}")


def single_collection():
    """Run a single data collection cycle"""
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'traffic_db',
        'user': 'traffic_user',
        'password': 'traffic123'
    }
    
    pipeline = TrafficDataPipeline(db_config)
    pipeline.run_collection_cycle()


if __name__ == "__main__":
    print("\n" + "="*80)
    print("TRAFFIC DATA TO POSTGRESQL PIPELINE")
    print("="*80)
    print("\nOptions:")
    print("1. Run single collection")
    print("2. Run continuous collection (every 5 minutes)")
    print("3. Run continuous collection (every 10 minutes)")
    print("4. Run continuous collection (every 1 minute - for testing)")
    
    choice = input("\nEnter choice (1/2/3/4): ").strip()
    
    if choice == "1":
        single_collection()
    elif choice == "2":
        continuous_collection(interval_minutes=5)
    elif choice == "3":
        continuous_collection(interval_minutes=10)
    elif choice == "4":
        continuous_collection(interval_minutes=1)
    else:
        print("Invalid choice. Running single collection...")
        single_collection()