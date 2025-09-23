FROM python:3.9.7-slim

WORKDIR /app
COPY filter/main.py /app/
COPY filter/filters.py /app/
COPY middleware/ /app/middleware/
RUN pip install --no-cache-dir pika
ENTRYPOINT ["python3", "main.py"]
