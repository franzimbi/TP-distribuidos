FROM python:3.9.7-alpine

WORKDIR /app
COPY healthchecker/main.py /app/
COPY healthchecker/healthchecker.py /app/

RUN apk update
RUN apk add docker

ENTRYPOINT ["python3", "main.py"]
