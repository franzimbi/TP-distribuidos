FROM python:3.11-slim
WORKDIR /app
COPY middleware /app/middleware
COPY test/exchange/producer/main.py /app/main.py
RUN pip install --no-cache-dir pika==1.3.2
CMD ["python", "/app/main.py"]
