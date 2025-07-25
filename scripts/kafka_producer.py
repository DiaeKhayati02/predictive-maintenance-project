# producer.py
import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime
import time

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9093'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1)
            )
            producer.close()
            print("Kafka is ready")
            break
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

def main():
    print("Starting Kafka producer...")
    
    # Wait for Kafka to be ready
    wait_for_kafka()
    
    # Read CSV file
    try:
        df = pd.read_csv('data/test.csv')
        print(f"Loaded {len(df)} records from CSV")
    except FileNotFoundError:
        print("Error: data/test.csv not found")
        return
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9093'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    
    print("Kafka producer connected successfully")
    
    # Send data
    for index, row in df.iterrows():
        row_dict = row.to_dict()
        row_dict['timestamp'] = datetime.now().isoformat()
        
        try:
            future = producer.send('test_data', row_dict)
            # Wait for message to be sent
            future.get(timeout=10)
            print(f"Sent record {index + 1}/{len(df)}")
        except Exception as e:
            print(f"Error sending record {index + 1}: {e}")
    
    producer.flush()
    producer.close()
    print("All messages sent successfully")

if __name__ == "__main__":
    main()