import requests
import cx_Oracle
from datetime import datetime
import time
import json

class TrafficDataPipeline:
    """
    Complete traffic data pipeline:
    1. Fetch from NYC Open Data
    2. Store in Oracle Database
    3. Provide analytics
    """
    
    def __init__(self, db_config):
        """
        Initialize with Oracle DB connection config
        
        Args:
            db_config: dict with keys: user, password, host, port, service_name
        """
        self.api_url = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
        self.db_config = db_config
        self.connection = None
        
    def connect_db(self):
        """Establish connection to Oracle Database"""
        try:
            dsn = cx_Oracle.makedsn(
                self.db_config['host'],
                self.db_config['port'],
                service_name=self.db_config['service_name']
            )
            self.connection = cx_Oracle.connect(
                user=self.db_config['user'],
                password=self.db_config['password'],
                dsn=dsn
            )
            print("✓ Connected to Oracle Database")
            return True
        except cx_Oracle.Error as error:
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
        Insert traffic records into Oracle Database
        Uses batch insert for performance
        """
        if not self.connection:
            print("❌ No database connection")
            return 0
        
        cursor = self.connection.cursor()
        
        # SQL INSERT with MERGE to handle duplicates
        insert_sql = """
            MERGE INTO traffic_data t
            USING (
                SELECT 
                    :1 AS time_stamp,
                    :2 AS link_id,
                    :3 AS link_name,
                    :4 AS borough,
                    :5 AS speed,
                    :6 AS travel_time,
                    :7 AS status,
                    :8 AS data_as_of,
                    :9 AS owner
                FROM DUAL
            ) s
            ON (t.link_id = s.link_id AND t.time_stamp = s.time_stamp)
            WHEN NOT MATCHED THEN
                INSERT (time_stamp, link_id, link_name, borough, speed, 
                        travel_time, status, data_as_of, owner)
                VALUES (s.time_stamp, s.link_id, s.link_name, s.borough, s.speed,
                        s.travel_time, s.status, s.data_as_of, s.owner)
        """
        
        # Prepare batch data
        batch_data = []
        for record in records:
            # Parse timestamp
            try:
                ts = datetime.fromisoformat(record.get('data_as_of', '').replace('Z', '+00:00'))
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
            # Execute batch insert
            cursor.executemany(insert_sql, batch_data)
            self.connection.commit()
            inserted = cursor.rowcount
            cursor.close()
            print(f"✓ Inserted/Updated {inserted} records")
            return inserted
        except cx_Oracle.Error as error:
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
        
        # Records by borough
        cursor.execute("""
            SELECT borough, COUNT(*) as count
            FROM traffic_data
            GROUP BY borough
            ORDER BY count DESC
        """)
        stats['by_borough'] = cursor.fetchall()
        
        # Average speed by borough (recent data)
        cursor.execute("""
            SELECT borough, ROUND(AVG(speed), 2) as avg_speed
            FROM traffic_data
            WHERE time_stamp >= SYSTIMESTAMP - INTERVAL '1' HOUR
            AND speed > 0
            GROUP BY borough
            ORDER BY avg_speed
        """)
        stats['avg_speed_by_borough'] = cursor.fetchall()
        
        cursor.close()
        return stats
    
    def print_statistics(self, stats):
        """Print database statistics"""
        print(f"\n{'='*80}")
        print(f"DATABASE STATISTICS")
        print(f"{'='*80}\n")
        
        print(f"📊 OVERALL")
        print(f"Total records: {stats['total_records']:,}")
        print(f"Data range: {stats['earliest_data']} to {stats['latest_data']}")
        print()
        
        print(f"🗽 RECORDS BY BOROUGH")
        for borough, count in stats['by_borough']:
            print(f"  {borough}: {count:,}")
        print()
        
        print(f"🚗 AVERAGE SPEED (Last Hour)")
        for borough, avg_speed in stats['avg_speed_by_borough']:
            print(f"  {borough}: {avg_speed} mph")
        print()
    
    def run_collection_cycle(self):
        """Run one complete data collection cycle"""
        print(f"\n{'='*80}")
        print(f"STARTING DATA COLLECTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
        # Fetch data
        print("1. Fetching traffic data from NYC Open Data...")
        raw_data = self.fetch_traffic_data(limit=1000)
        
        if not raw_data:
            print("❌ Failed to fetch data")
            return False
        
        print(f"✓ Fetched {len(raw_data)} records")
        
        # Connect to DB
        print("\n2. Connecting to Oracle Database...")
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
        
        print(f"\n{'='*80}")
        print(f"COLLECTION CYCLE COMPLETE")
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
        'user': 'traffic_user',
        'password': 'TrafficPass123',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'XEPDB1'
    }
    
    pipeline = TrafficDataPipeline(db_config)
    
    print("="*80)
    print("TRAFFIC DATA COLLECTION PIPELINE")
    print("="*80)
    print(f"Collection interval: {interval_minutes} minutes")
    print(f"Target: Oracle Database XE at {db_config['host']}:{db_config['port']}")
    print(f"Press Ctrl+C to stop")
    print("="*80)
    
    cycle_count = 0
    
    try:
        while True:
            cycle_count += 1
            print(f"\n\n>>> CYCLE {cycle_count} <<<")
            
            success = pipeline.run_collection_cycle()
            
            if success:
                print(f"\n⏰ Sleeping for {interval_minutes} minutes...")
                print(f"Next collection at: {datetime.now() + timedelta(minutes=interval_minutes)}")
                time.sleep(interval_minutes * 60)
            else:
                print("\n⚠️  Error in cycle, retrying in 1 minute...")
                time.sleep(60)
                
    except KeyboardInterrupt:
        print("\n\n✓ Stopped by user")
        print(f"Total cycles completed: {cycle_count}")


# One-time collection
def single_collection():
    """Run a single data collection cycle"""
    from datetime import timedelta
    
    db_config = {
        'user': 'traffic_user',
        'password': 'TrafficPass123',
        'host': 'localhost',
        'port': 1521,
        'service_name': 'XEPDB1'
    }
    
    pipeline = TrafficDataPipeline(db_config)
    pipeline.run_collection_cycle()


if __name__ == "__main__":
    import sys
    from datetime import timedelta
    
    print("Traffic Data to Oracle Pipeline")
    print("1. Run single collection")
    print("2. Run continuous collection (every 5 minutes)")
    print("3. Run continuous collection (every 10 minutes)")
    
    choice = input("\nEnter choice (1/2/3): ").strip()
    
    if choice == "1":
        single_collection()
    elif choice == "2":
        continuous_collection(interval_minutes=5)
    elif choice == "3":
        continuous_collection(interval_minutes=10)
    else:
        print("Invalid choice. Running single collection...")
        single_collection()