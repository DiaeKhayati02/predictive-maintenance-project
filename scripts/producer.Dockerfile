FROM python:3.9-slim

WORKDIR /app

COPY scripts/kafka_producer.py .

RUN pip install pandas kafka-python

CMD ["python", "kafka_producer.py"]
