# consumer.py
from kafka import KafkaConsumer
import psycopg2
import json
import time

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=['kafka:9093'],
                api_version=(0, 10, 1),
                consumer_timeout_ms=1000
            )
            consumer.close()
            print("Kafka is ready")
            break
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

def wait_for_postgres():
    """Wait for PostgreSQL to be ready"""
    while True:
        try:
            conn = psycopg2.connect(
                dbname="maintenance_db", 
                user="admin", 
                password="password", 
                host="postgres"
            )
            conn.close()
            print("PostgreSQL is ready")
            break
        except Exception as e:
            print(f"Waiting for PostgreSQL... {e}")
            time.sleep(5)

def create_table():
    """Create the raw_data table if it doesn't exist"""
    conn = psycopg2.connect(
        dbname="maintenance_db", 
        user="admin", 
        password="password", 
        host="postgres"
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(20),
            air_temperature FLOAT,
            process_temperature FLOAT,
            rotational_speed FLOAT,
            torque FLOAT,
            tool_wear FLOAT,
            failure_type VARCHAR(50),
            timestamp TIMESTAMP
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Table created/verified")

def main():
    print("Starting Kafka consumer...")
    
    # Wait for services to be ready
    wait_for_kafka()
    wait_for_postgres()
    
    # Create table
    create_table()
    
    # Start consuming
    consumer = KafkaConsumer(
        'test_data', 
        bootstrap_servers=['kafka:9093'], 
        api_version=(0, 10, 1),
        auto_offset_reset='earliest'  # Start from beginning
    )
    
    conn = psycopg2.connect(
        dbname="maintenance_db", 
        user="admin", 
        password="password", 
        host="postgres"
    )
    cur = conn.cursor()
    
    print("Consumer started, waiting for messages...")
    
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
            data = json.loads(message.value)
            
            cur.execute(
                """
                INSERT INTO raw_data (product_id, air_temperature, process_temperature, rotational_speed, torque, tool_wear, failure_type, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    data.get('Product ID', ''),
                    data.get('Air temperature [K]', 0),
                    data.get('Process temperature [K]', 0),
                    data.get('Rotational speed [rpm]', 0),
                    data.get('Torque [Nm]', 0),
                    data.get('Tool wear [min]', 0),
                    'Failure' if data.get('failure', 0) == 1 else 'No Failure',
                    data['timestamp']
                )
            )
            raw_data_id = cur.fetchone()[0]
            conn.commit()
            print(f"Inserted record with ID: {raw_data_id}")
            
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()