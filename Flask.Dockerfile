FROM python:3.9-slim

WORKDIR /app

COPY req2.0.txt .
RUN pip install -r req2.0.txt

COPY . .

EXPOSE 5000

CMD ["python", "app.py"]