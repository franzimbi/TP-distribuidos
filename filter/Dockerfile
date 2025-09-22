FROM python:3.9.7-slim

WORKDIR /app
COPY main.py /app/
COPY filters.py /app/
RUN pip install --no-cache-dir pika
ENTRYPOINT ["python3", "main.py"]
