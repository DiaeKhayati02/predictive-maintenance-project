from kafka import KafkaProducer
import pandas as pd
import json
import time
import socket

def wait_for_kafka():
    """Wait for Kafka to be ready"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9093'],
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
    
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9093'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    
    # Read CSV file
    df = pd.read_csv('./data/validation_data_with_timestamps.csv')
    
    # Send each row to Kafka
    for index, row in df.iterrows():
        # Convert row to dictionary, ensuring all columns are included
        data = row.to_dict()
        # Ensure datetime and timestamp are strings (JSON-serializable)
        if 'datetime' in data:
            data['datetime'] = str(data['datetime'])
        if 'timestamp' in data:
            data['timestamp'] = str(data['timestamp'])
        
        try:
            producer.send('test_data', value=data)
            print(f"Sent record {index + 1}/{len(df)}")
            time.sleep(0.1)  # Small delay to avoid overwhelming Kafka
        except Exception as e:
            print(f"Error sending record {index + 1}: {e}")
    
    # Ensure all messages are sent
    producer.flush()
    producer.close()
    print("All records sent. Producer closed.")

if __name__ == "__main__":
    main()