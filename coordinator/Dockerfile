FROM python:3.9.7-slim

WORKDIR /app
COPY coordinator/main.py /app/
COPY coordinator/coordinator.py /app/
COPY common/ /app/common/
COPY middleware/ /app/middleware/
RUN pip install --no-cache-dir pika
ENV PYTHONPATH=/app
ENTRYPOINT ["python3", "main.py"]