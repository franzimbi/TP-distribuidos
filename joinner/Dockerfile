FROM python:3.9.7-slim

WORKDIR /app
COPY joinner/main.py /app/
COPY joinner/join.py /app/
COPY common/ /app/common/
COPY middleware/ /app/middleware/
RUN pip install --no-cache-dir pika
ENV PYTHONPATH=/app
ENTRYPOINT ["python3", "main.py"]
