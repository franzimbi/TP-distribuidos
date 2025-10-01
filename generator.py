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
        for j in range(1, n + 1):
            join_queues.append(f"JOIN_QUEUE_Q4.{i}.{j}=j4{i}{j}")
    join_queues[0] = "- " + join_queues[0]
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


def crear_filtros_q1(name, n, type_filter):
    result = ""
    for i in range(1, n + 1):
        result += f"""
 filter-q1.f{name}.{i}:
  build:
   context: .
   dockerfile: filter/Dockerfile
  restart: on-failure
  environment:
   - PYTHONUNBUFFERED=1
   - CONSUME_QUEUE=q1{name}
   - PRODUCE_QUEUE=q1{str(int(name)+1)}

   - COORDINATOR_PRODUCE_QUEUE=Q1C{name}
   - COORDINATOR_CONSUME_QUEUE=F{name}{i}

   - FILTER_NAME={type_filter}
  networks:
   - mynet
  volumes:
   - ./config.ini:/app/config.ini
"""
    # coodinador

    result += f"""
 coordinator-q1.{name}:
  build:
   context: .
   dockerfile: coordinator/Dockerfile
  restart: on-failure
  depends_on:
"""
    aux = "\n".join([f"    - filter-q1.f{name}.{i}" for i in range(1, n+1)])
    result += aux
    result += f"""
  environment:
   - PYTHONUNBUFFERED=1
   - NUM_NODES={n}
   - CONSUME_QUEUE=Q1C1
"""
    produces_q = "\n".join([f"    - PRODUCE_QUEUE_{i}=F{name}{i}" for i in range(1, n+1)])
    result += produces_q
    result += f"""
   - DOWNSTREAM_QUEUE=q12
  networks:
   - mynet
  volumes:
   - ./config.ini:/app/config.ini
"""
    return result


with open('text.yaml', 'w') as f:
    f.write('services:\n')
    f.write(crear_distributor(2))
    f.write(crear_filtros_q1(1, 2, 'bytime'))
    f.write(crear_filtros_q1(2, 2, 'byamount'))
