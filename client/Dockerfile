FROM python:3.9.7-slim

WORKDIR /app
COPY client/main.py /app/
COPY middleware/ /app/middleware/
RUN pip install --no-cache-dir pika
ENV PYTHONPATH=/app
ENTRYPOINT ["python3", "main.py"]
