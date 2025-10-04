#!/usr/bin/env python3
import sys
import yaml


# if len(sys.argv) != 2:
#     print("Uso: python3 generador.py N")
#     sys.exit(1)

def crear_distributor():
    data = {
        'build': {
            'context': '.',
            'dockerfile': 'distributor/Dockerfile',
        },
        'restart': 'on-failure',
        'environment': [
            'PYTHONUNBUFFERED = 1',
            'Q1result=Q1result',
            'Q21result=Q21result',
            'Q22result=Q22result',
            'Q3result=Q3result',
            'Q4result=Q4result',
            'transactionsExchange=transactionsExchange',
            'itemsExchange=itemsExchange',
            'productsExchange=productsExchange',
            'storesExchange=storesExchange',
            'usersExchange=usersExchange',
        ],
        'networks': [
            'mynet'
        ],
        'volumes': [
            './config.ini:/app/config.ini'
        ],
    }
    return data


def crear_filters(nombre, cantidad, entrada, salida, type):
    filters = {}
    queues_to_coordinator = ''
    for i in range(1, cantidad + 1):
        filter_name = f'filter{nombre}_{i}'
        conection_coordinator = f'{filter_name}_receive_from_coordinator_{nombre}'
        queues_to_coordinator += f',{conection_coordinator}'
        filters[filter_name] = {
            'build': {
                'context': '.',
                'dockerfile': 'filter/Dockerfile',
            },
            'depends_on': [
                'distributor'
            ],
            'restart': 'on-failure',
            'environment': [
                'PYTHONUNBUFFERED=1',
                'tipoEntrada=' + entrada,
                'queueEntrada=entradaFilter' + nombre + '_' + str(i),
                'tipoSalida=queue',
                # 'queuesSalida=' + salida,
                'queue_to_send_coordinator=coodinator_' + str(nombre) + '_' + 'unique_queue',
                'queue_to_receive_coordinator=' + conection_coordinator,
                'filter_name=' + type
            ],
            'networks': [
                'mynet'
            ],
            'volumes': [
                './config.ini:/app/config.ini'
            ],
        }
    coordinator_name = f'coordinator_{nombre}'
    filters[coordinator_name] = {
        'build': {
            'context': '.',
            'dockerfile': 'filter/Dockerfile',
        },
        'restart': 'on-failure',
        'environment': [
            'PYTHONUNBUFFERED=1',
            'consume_queue=coodinator_' + str(nombre) + '_' + 'unique_queue',
            'num_nodes=' + str(cantidad),
            'queues_to_send_to_nodes=' + queues_to_coordinator,
            # 'tipoSalida=queue',
            'queue_to_send_last_batch=' + salida,
        ],
        'networks': [
            'mynet'
        ],
        'volumes': [
            './config.ini:/app/config.ini'
        ],
    }

    return filters


with open('text.yaml', 'w') as f:
    services = {'distributor': crear_distributor()}
    services.update(crear_filters(nombre='Anio1', cantidad=2, entrada='exchange,itemsExchange',
                                  salida='Queue_begin2_1,Queue_begin2_2', type='byyear'))
    services.update(crear_filters(nombre='Anio2', cantidad=2, entrada='exchange,transactionExchange',
                                  salida='Queue_begin_4,Queue_begin_3_y_1', type='byyear'))
    services.update(crear_filters(nombre='Hora1', cantidad=2, entrada='queue,Queue_begin_3_y_1',
                                  salida='Queue_3, Queue_1', type='bytime'))
    data = {'services': services}
    yaml.dump(data, f, sort_keys=False, default_flow_style=False)
