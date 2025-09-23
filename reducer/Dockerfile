FROM python:3.9.7-slim

WORKDIR /app
COPY reducer/main.py /app/
COPY common/protocol.py /app/common/
COPY middleware/ /app/middleware/
RUN pip install --no-cache-dir pika
ENTRYPOINT ["python3", "main.py"]
