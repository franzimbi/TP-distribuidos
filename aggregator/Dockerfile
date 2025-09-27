FROM python:3.9.7-slim

WORKDIR /app

# Código del aggregator
COPY aggregator/main.py /app/
COPY aggregator/aggregator_tpv.py /app/

# Dependencias compartidas
COPY common/ /app/common/
COPY middleware/ /app/middleware/

# Lib de MQ
RUN pip install --no-cache-dir pika

ENTRYPOINT ["python3", "main.py"]
