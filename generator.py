#!/usr/bin/env python3
import sys
import yaml

# if len(sys.argv) != 2:
#     print("Uso: python3 generador.py N")
#     sys.exit(1)

def crear_distributor(n):
    join_queues = []
    for i in range(1, n + 1):
        join_queues.append(f"JOIN_QUEUE_Q3.{i}=j3{i}")
        join_queues.append(f"JOIN_QUEUE_Q4.{i}=j4{i}")
    join_queues[0] = "- "+ join_queues[0]
    join_envs = "\n   - ".join(join_queues)

    return f"""  
 distributor:
  build:
   context: .
   dockerfile: distributor/Dockerfile
  restart: on-failure
  environment:
   - PYTHONUNBUFFERED=1
   - PRODUCE_QUEUE_Q1=q11
   - CONSUME_QUEUE_Q1=q14
   - PRODUCE_QUEUE_Q3=q31
   - CONSUME_QUEUE_Q3=q34
   - PRODUCE_QUEUE_Q4=q41
   - CONSUME_QUEUE_Q4=q46
   {join_envs}
  networks:
   - mynet
  volumes:
   - ./config.ini:/app/config.ini
"""

def crear_filtro(name, n):



with open('text.yaml', 'w') as f:
    f.write("name: tp0\nservices:\n")
    f.write(crear_distributor(2))