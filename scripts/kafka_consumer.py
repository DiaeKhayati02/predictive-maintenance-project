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
    """Create the maintenance_data table if it doesn't exist"""
    conn = psycopg2.connect(
        dbname="maintenance_db", 
        user="admin", 
        password="password", 
        host="postgres"
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS maintenance_data (
            id SERIAL PRIMARY KEY,
            datetime TIMESTAMP,
            machine_id INTEGER,
            sample_id INTEGER,
            pressure_t0 FLOAT,
            hour_0_t0 FLOAT, hour_1_t0 FLOAT, hour_2_t0 FLOAT, hour_3_t0 FLOAT, hour_4_t0 FLOAT,
            hour_5_t0 FLOAT, hour_6_t0 FLOAT, hour_7_t0 FLOAT, hour_8_t0 FLOAT, hour_9_t0 FLOAT,
            hour_10_t0 FLOAT, hour_11_t0 FLOAT, hour_12_t0 FLOAT, hour_13_t0 FLOAT, hour_14_t0 FLOAT,
            hour_15_t0 FLOAT, hour_16_t0 FLOAT, hour_17_t0 FLOAT, hour_18_t0 FLOAT, hour_19_t0 FLOAT,
            hour_20_t0 FLOAT, hour_21_t0 FLOAT, hour_22_t0 FLOAT, hour_23_t0 FLOAT,
            pressure_t1 FLOAT,
            hour_0_t1 FLOAT, hour_1_t1 FLOAT, hour_2_t1 FLOAT, hour_3_t1 FLOAT, hour_4_t1 FLOAT,
            hour_5_t1 FLOAT, hour_6_t1 FLOAT, hour_7_t1 FLOAT, hour_8_t1 FLOAT, hour_9_t1 FLOAT,
            hour_10_t1 FLOAT, hour_11_t1 FLOAT, hour_12_t1 FLOAT, hour_13_t1 FLOAT, hour_14_t1 FLOAT,
            hour_15_t1 FLOAT, hour_16_t1 FLOAT, hour_17_t1 FLOAT, hour_18_t1 FLOAT, hour_19_t1 FLOAT,
            hour_20_t1 FLOAT, hour_21_t1 FLOAT, hour_22_t1 FLOAT, hour_23_t1 FLOAT,
            pressure_t2 FLOAT,
            hour_0_t2 FLOAT, hour_1_t2 FLOAT, hour_2_t2 FLOAT, hour_3_t2 FLOAT, hour_4_t2 FLOAT,
            hour_5_t2 FLOAT, hour_6_t2 FLOAT, hour_7_t2 FLOAT, hour_8_t2 FLOAT, hour_9_t2 FLOAT,
            hour_10_t2 FLOAT, hour_11_t2 FLOAT, hour_12_t2 FLOAT, hour_13_t2 FLOAT, hour_14_t2 FLOAT,
            hour_15_t2 FLOAT, hour_16_t2 FLOAT, hour_17_t2 FLOAT, hour_18_t2 FLOAT, hour_19_t2 FLOAT,
            hour_20_t2 FLOAT, hour_21_t2 FLOAT, hour_22_t2 FLOAT, hour_23_t2 FLOAT,
            pressure_t3 FLOAT,
            hour_0_t3 FLOAT, hour_1_t3 FLOAT, hour_2_t3 FLOAT, hour_3_t3 FLOAT, hour_4_t3 FLOAT,
            hour_5_t3 FLOAT, hour_6_t3 FLOAT, hour_7_t3 FLOAT, hour_8_t3 FLOAT, hour_9_t3 FLOAT,
            hour_10_t3 FLOAT, hour_11_t3 FLOAT, hour_12_t3 FLOAT, hour_13_t3 FLOAT, hour_14_t3 FLOAT,
            hour_15_t3 FLOAT, hour_16_t3 FLOAT, hour_17_t3 FLOAT, hour_18_t3 FLOAT, hour_19_t3 FLOAT,
            hour_20_t3 FLOAT, hour_21_t3 FLOAT, hour_22_t3 FLOAT, hour_23_t3 FLOAT,
            pressure_t4 FLOAT,
            hour_0_t4 FLOAT, hour_1_t4 FLOAT, hour_2_t4 FLOAT, hour_3_t4 FLOAT, hour_4_t4 FLOAT,
            hour_5_t4 FLOAT, hour_6_t4 FLOAT, hour_7_t4 FLOAT, hour_8_t4 FLOAT, hour_9_t4 FLOAT,
            hour_10_t4 FLOAT, hour_11_t4 FLOAT, hour_12_t4 FLOAT, hour_13_t4 FLOAT, hour_14_t4 FLOAT,
            hour_15_t4 FLOAT, hour_16_t4 FLOAT, hour_17_t4 FLOAT, hour_18_t4 FLOAT, hour_19_t4 FLOAT,
            hour_20_t4 FLOAT, hour_21_t4 FLOAT, hour_22_t4 FLOAT, hour_23_t4 FLOAT,
            target_pressure FLOAT,
            timestamp TIMESTAMP
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Table maintenance_data created/verified")

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
                INSERT INTO maintenance_data (
                    datetime, machine_id, sample_id,
                    pressure_t0,
                    hour_0_t0, hour_1_t0, hour_2_t0, hour_3_t0, hour_4_t0,
                    hour_5_t0, hour_6_t0, hour_7_t0, hour_8_t0, hour_9_t0,
                    hour_10_t0, hour_11_t0, hour_12_t0, hour_13_t0, hour_14_t0,
                    hour_15_t0, hour_16_t0, hour_17_t0, hour_18_t0, hour_19_t0,
                    hour_20_t0, hour_21_t0, hour_22_t0, hour_23_t0,
                    pressure_t1,
                    hour_0_t1, hour_1_t1, hour_2_t1, hour_3_t1, hour_4_t1,
                    hour_5_t1, hour_6_t1, hour_7_t1, hour_8_t1, hour_9_t1,
                    hour_10_t1, hour_11_t1, hour_12_t1, hour_13_t1, hour_14_t1,
                    hour_15_t1, hour_16_t1, hour_17_t1, hour_18_t1, hour_19_t1,
                    hour_20_t1, hour_21_t1, hour_22_t1, hour_23_t1,
                    pressure_t2,
                    hour_0_t2, hour_1_t2, hour_2_t2, hour_3_t2, hour_4_t2,
                    hour_5_t2, hour_6_t2, hour_7_t2, hour_8_t2, hour_9_t2,
                    hour_10_t2, hour_11_t2, hour_12_t2, hour_13_t2, hour_14_t2,
                    hour_15_t2, hour_16_t2, hour_17_t2, hour_18_t2, hour_19_t2,
                    hour_20_t2, hour_21_t2, hour_22_t2, hour_23_t2,
                    pressure_t3,
                    hour_0_t3, hour_1_t3, hour_2_t3, hour_3_t3, hour_4_t3,
                    hour_5_t3, hour_6_t3, hour_7_t3, hour_8_t3, hour_9_t3,
                    hour_10_t3, hour_11_t3, hour_12_t3, hour_13_t3, hour_14_t3,
                    hour_15_t3, hour_16_t3, hour_17_t3, hour_18_t3, hour_19_t3,
                    hour_20_t3, hour_21_t3, hour_22_t3, hour_23_t3,
                    pressure_t4,
                    hour_0_t4, hour_1_t4, hour_2_t4, hour_3_t4, hour_4_t4,
                    hour_5_t4, hour_6_t4, hour_7_t4, hour_8_t4, hour_9_t4,
                    hour_10_t4, hour_11_t4, hour_12_t4, hour_13_t4, hour_14_t4,
                    hour_15_t4, hour_16_t4, hour_17_t4, hour_18_t4, hour_19_t4,
                    hour_20_t4, hour_21_t4, hour_22_t4, hour_23_t4,
                    target_pressure, timestamp
                ) VALUES (
                    %s, %s, %s,
                    %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
                RETURNING id
                """,
                (
                    data.get('datetime', None),
                    data.get('machine_id', 0),
                    data.get('sample_id', 0),
                    data.get('pressure_t0', 0),
                    data.get('hour_0_t0', 0), data.get('hour_1_t0', 0), data.get('hour_2_t0', 0), data.get('hour_3_t0', 0), data.get('hour_4_t0', 0),
                    data.get('hour_5_t0', 0), data.get('hour_6_t0', 0), data.get('hour_7_t0', 0), data.get('hour_8_t0', 0), data.get('hour_9_t0', 0),
                    data.get('hour_10_t0', 0), data.get('hour_11_t0', 0), data.get('hour_12_t0', 0), data.get('hour_13_t0', 0), data.get('hour_14_t0', 0),
                    data.get('hour_15_t0', 0), data.get('hour_16_t0', 0), data.get('hour_17_t0', 0), data.get('hour_18_t0', 0), data.get('hour_19_t0', 0),
                    data.get('hour_20_t0', 0), data.get('hour_21_t0', 0), data.get('hour_22_t0', 0), data.get('hour_23_t0', 0),
                    data.get('pressure_t1', 0),
                    data.get('hour_0_t1', 0), data.get('hour_1_t1', 0), data.get('hour_2_t1', 0), data.get('hour_3_t1', 0), data.get('hour_4_t1', 0),
                    data.get('hour_5_t1', 0), data.get('hour_6_t1', 0), data.get('hour_7_t1', 0), data.get('hour_8_t1', 0), data.get('hour_9_t1', 0),
                    data.get('hour_10_t1', 0), data.get('hour_11_t1', 0), data.get('hour_12_t1', 0), data.get('hour_13_t1', 0), data.get('hour_14_t1', 0),
                    data.get('hour_15_t1', 0), data.get('hour_16_t1', 0), data.get('hour_17_t1', 0), data.get('hour_18_t1', 0), data.get('hour_19_t1', 0),
                    data.get('hour_20_t1', 0), data.get('hour_21_t1', 0), data.get('hour_22_t1', 0), data.get('hour_23_t1', 0),
                    data.get('pressure_t2', 0),
                    data.get('hour_0_t2', 0), data.get('hour_1_t2', 0), data.get('hour_2_t2', 0), data.get('hour_3_t2', 0), data.get('hour_4_t2', 0),
                    data.get('hour_5_t2', 0), data.get('hour_6_t2', 0), data.get('hour_7_t2', 0), data.get('hour_8_t2', 0), data.get('hour_9_t2', 0),
                    data.get('hour_10_t2', 0), data.get('hour_11_t2', 0), data.get('hour_12_t2', 0), data.get('hour_13_t2', 0), data.get('hour_14_t2', 0),
                    data.get('hour_15_t2', 0), data.get('hour_16_t2', 0), data.get('hour_17_t2', 0), data.get('hour_18_t2', 0), data.get('hour_19_t2', 0),
                    data.get('hour_20_t2', 0), data.get('hour_21_t2', 0), data.get('hour_22_t2', 0), data.get('hour_23_t2', 0),
                    data.get('pressure_t3', 0),
                    data.get('hour_0_t3', 0), data.get('hour_1_t3', 0), data.get('hour_2_t3', 0), data.get('hour_3_t3', 0), data.get('hour_4_t3', 0),
                    data.get('hour_5_t3', 0), data.get('hour_6_t3', 0), data.get('hour_7_t3', 0), data.get('hour_8_t3', 0), data.get('hour_9_t3', 0),
                    data.get('hour_10_t3', 0), data.get('hour_11_t3', 0), data.get('hour_12_t3', 0), data.get('hour_13_t3', 0), data.get('hour_14_t3', 0),
                    data.get('hour_15_t3', 0), data.get('hour_16_t3', 0), data.get('hour_17_t3', 0), data.get('hour_18_t3', 0), data.get('hour_19_t3', 0),
                    data.get('hour_20_t3', 0), data.get('hour_21_t3', 0), data.get('hour_22_t3', 0), data.get('hour_23_t3', 0),
                    data.get('pressure_t4', 0),
                    data.get('hour_0_t4', 0), data.get('hour_1_t4', 0), data.get('hour_2_t4', 0), data.get('hour_3_t4', 0), data.get('hour_4_t4', 0),
                    data.get('hour_5_t4', 0), data.get('hour_6_t4', 0), data.get('hour_7_t4', 0), data.get('hour_8_t4', 0), data.get('hour_9_t4', 0),
                    data.get('hour_10_t4', 0), data.get('hour_11_t4', 0), data.get('hour_12_t4', 0), data.get('hour_13_t4', 0), data.get('hour_14_t4', 0),
                    data.get('hour_15_t4', 0), data.get('hour_16_t4', 0), data.get('hour_17_t4', 0), data.get('hour_18_t4', 0), data.get('hour_19_t4', 0),
                    data.get('hour_20_t4', 0), data.get('hour_21_t4', 0), data.get('hour_22_t4', 0), data.get('hour_23_t4', 0),
                    data.get('target_pressure', 0),
                    data.get('timestamp', None)
                )
            )
            maintenance_data_id = cur.fetchone()[0]
            conn.commit()
            print(f"Inserted record with ID: {maintenance_data_id}")
            
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()