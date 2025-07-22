import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime

df = pd.read_csv('data/test.csv')
producer = KafkaProducer(
    bootstrap_servers=['kafka:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10, 1)
)
print("Kafka producer connected successfully")
for _, row in df.iterrows():
    row_dict = row.to_dict()
    row_dict['timestamp'] = datetime.now().isoformat()
    producer.send('test_data', row_dict)
producer.flush()