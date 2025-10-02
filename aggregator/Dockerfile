FROM python:3.9.7-slim

WORKDIR /app

COPY aggregator/main.py /app/
COPY aggregator/aggregator_tpv.py /app/
COPY aggregator/counter.py /app/
COPY common/ /app/common/
COPY middleware/ /app/middleware/
RUN pip install --no-cache-dir pika

ENTRYPOINT ["python3", "main.py"]
