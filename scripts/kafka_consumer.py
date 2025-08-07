from kafka import KafkaConsumer
import psycopg2
import json
import time
import socket

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
    print("Starting PostgreSQL connection attempt...")
    time.sleep(10)  # Initial delay to ensure DNS is ready
    attempt = 1
    while True:
        try:
            # Try resolving the hostname
            ip_address = socket.gethostbyname('postgres')
            print(f"Attempt {attempt}: Resolved 'postgres' to IP: {ip_address}")
            
            # Attempt PostgreSQL connection
            conn = psycopg2.connect(
                dbname="maintenance_db", 
                user="admin", 
                password="password", 
                host="postgres",
                connect_timeout=5
            )
            conn.close()
            print("PostgreSQL is ready")
            break
        except socket.gaierror as e:
            print(f"Attempt {attempt}: DNS resolution failed for 'postgres': {e}")
            time.sleep(5)
        except psycopg2.OperationalError as e:
            print(f"Attempt {attempt}: PostgreSQL connection failed: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"Attempt {attempt}: Unexpected error: {e.__class__.__name__}: {str(e)}")
            time.sleep(5)
        attempt += 1

def create_table():
    """Create the machine_data table if it doesn't exist"""
    conn = psycopg2.connect(
        dbname="maintenance_db", 
        user="admin", 
        password="password", 
        host="postgres"
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS machine_data (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP,
            machine_id INTEGER,
            volt FLOAT,
            rotate FLOAT,
            pressure FLOAT,
            vibration FLOAT
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Table machine_data created/verified")

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
                INSERT INTO machine_data (
                    datetime, machine_id, 
                    volt,
                    rotate,
                    pressure,
                    vibration
                ) VALUES (
                    %s, %s, %s,
                    %s,
                    %s, %s
                )
                RETURNING id
                """,
                (
                    data.get('datetime', None),
                    data.get('machine_id', 0),
                    data.get('volt', 0),
                    data.get('rotate', 0),
                    data.get('pressure', 0),
                    data.get('vibration', 0)
                    
                )
            )
            machine_data_id = cur.fetchone()[0]
            conn.commit()
            print(f"Inserted record with ID: {machine_data_id}")
            
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()