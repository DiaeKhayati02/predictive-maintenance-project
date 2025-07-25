FROM python:3.9-slim

WORKDIR /app

COPY scripts/kafka_consumer.py .

RUN pip install kafka-python psycopg2-binary

CMD ["python", "kafka_consumer.py"]
