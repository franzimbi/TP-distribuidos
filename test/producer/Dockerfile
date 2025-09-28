FROM python:3.9.7-slim

WORKDIR /app
COPY test/producer/main.py /app/
COPY middleware/middleware.py /app/
RUN pip install --no-cache-dir pika
ENTRYPOINT ["python3", "main.py"]