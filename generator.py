#!/usr/bin/env python3
import sys
import yaml

if len(sys.argv) != 3:
    print("Uso: python3 generador.py <cantidad nodos> <cantidad clientes>")
    sys.exit(1)

cant_nodos = int(sys.argv[1])
cant_clientes = int(sys.argv[2])
nombre_file = 'docker-compose-dev.yaml2'


def crear_distributor():
    data = {
        'build': {
            'context': '.',
            'dockerfile': 'distributor/Dockerfile',
        },
        'restart': 'on-failure',
        'environment': [
            'PYTHONUNBUFFERED=1',
            'Q1result=Queue_final_Q1',
            'Q21result=Queue_final_Q21',
            'Q22result=Queue_final_Q22',
            'Q3result=Queue_final_Q3',
            'Q4result=Queue_final_Q4',
            'transactionsQueue=transaction_queue',
            'itemsQueue=items_queue',
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
    for i in range(1, cantidad + 1):
        filter_name = f'{nombre}_{i}'
        filters[filter_name.lower()] = {
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
                'CONSUME_QUEUE=' + entrada,
                'PRODUCE_QUEUE=' + salida,
                'FILTER_NAME=' + type
            ],
            'networks': [
                'mynet'
            ],
            'volumes': [
                './config.ini:/app/config.ini'
            ],
        }

    return filters


def crear_aggregators(nombre, cantidad, entrada, salida, type, params):
    if type == 'accumulator' and cantidad != 1:
        raise TypeError('Cantidad invalido para un accumulator')
    aggregators = {}
    for i in range(1, cantidad + 1):
        aggregator_name = f'aggregator{nombre}_{i}'
        aggregators[aggregator_name.lower()] = {
            'build': {
                'context': '.',
                'dockerfile': 'aggregator/Dockerfile',
            },
            'depends_on': [
                'distributor'
            ],
            'restart': 'on-failure',
            'environment': [
                'PYTHONUNBUFFERED=1',
                'CONSUME_QUEUE=' + entrada,
                'PRODUCE_QUEUE=' + salida,
                'TYPE=' + type,
                'PARAMS=' + params,
            ],
            'networks': [
                'mynet'
            ],
            'volumes': [
                './config.ini:/app/config.ini'
            ],
        }
    return aggregators


def crear_reducers(nombre, entrada, salida, top, params):
    reducers = {}
    reducer_name = f'reducer{nombre}'
    reducers[reducer_name.lower()] = {
        'build': {
            'context': '.',
            'dockerfile': 'reducer/Dockerfile',
        },
        'depends_on': [
            'distributor'
        ],
        'restart': 'on-failure',
        'environment': [
            'PYTHONUNBUFFERED=1',
            'CONSUME_QUEUE=' + entrada,
            'PRODUCE_QUEUE=' + salida,
            'TOP=' + str(top),
            'PARAMS=' + params
        ],
        'networks': [
            'mynet'
        ],
        'volumes': [
            './config.ini:/app/config.ini'
        ],
    }
    return reducers


def crear_joiners(nombre, cantidad, entradaJoin, entradaData, salida, disk_type, params):
    joiners = {}
    for i in range(1, cantidad + 1):
        joiner_name = f'{nombre}_{i}'
        joiners[joiner_name.lower()] = {
            'build': {
                'context': '.',
                'dockerfile': 'joinner/Dockerfile',
            },
            'depends_on': [
                'distributor'
            ],
            'restart': 'on-failure',
            'environment': [
                'PYTHONUNBUFFERED=1',
                'entradaJoin=' + entradaJoin,
                'queueEntradaJoin=entradaJoiner' + nombre + '_' + str(i),
                'queueEntradaData=' + entradaData,
                'queuesSalida=' + salida,
                'use_diskcache=' + str(disk_type),
                'params=' + params
            ],
            'networks': [
                'mynet'
            ],
            'volumes': [
                './config.ini:/app/config.ini'
            ],
        }
    return joiners


def crear_client(cantidad, puerto):
    clients = {}
    for i in range(1, cantidad + 1):
        client_name = f'cliente_{i}'
        clients[client_name] = {
            'build': {
                'context': '.',
                'dockerfile': 'client/Dockerfile',
            },
            'restart': 'on-failure',
            'depends_on': [
                'distributor'
            ],
            'links': [
                'distributor'
            ],
            'environment': [
                'PYTHONUNBUFFERED=1',
                'DISTRIBUTOR_HOST=distributor',
                'DISTRIBUTOR_PORT=' + str(puerto),
            ],
            'volumes': [
                './config.ini:/app/config.ini',
                './csvs_files:/app/csvs_files',
                './csvs_files_reduced:/app/csvs_files_reduced',
                './csvs_files_juguete:/app/csvs_files_juguete',
                './results' + '_' + client_name + ':/app/results'
            ],
            'networks': [
                'mynet'
            ],
        }
    return clients


with open(nombre_file, 'w') as f:
    services = {'distributor': crear_distributor()}
    services.update(crear_filters(nombre='FiltroAnio1', cantidad=cant_nodos, entrada='items_queue',
                                  salida='Queue_begin2_1,Queue_begin2_2', type='byyear'))
    services.update(crear_filters(nombre='FiltroAnio2', cantidad=cant_nodos, entrada='transaction_queue',
                                  salida='Queue_begin_4,Queue_begin_3_y_1', type='byyear'))
    services.update(crear_filters(nombre='FiltroHora1', cantidad=cant_nodos, entrada='Queue_begin_3_y_1',
                                  salida='Queue_3,Queue_1', type='bytime'))
    services.update(crear_aggregators(nombre='Suma_Q21', cantidad=cant_nodos, entrada='Queue_begin2_1',
                                      salida='Queue_between_aggregator_reducer_Q21', type='sum',
                                      params='item_id,quantity,month,year_month,created_at,total_quantity'))
    services.update(crear_aggregators(nombre='Suma_Q22', cantidad=cant_nodos, entrada='Queue_begin2_2',
                                      salida='Queue_between_aggregator_reducer_Q22', type='sum',
                                      params='item_id,subtotal,month,year_month,created_at,total_earnings'))
    services.update(crear_aggregators(nombre='Suma_Q3', cantidad=cant_nodos, entrada='Queue_3',
                                      salida='Queue_between_aggregators_acumulator_Q3', type='sum',
                                      params='store_id,final_amount,semester,year_semester,created_at,tpv'))
    services.update(
        crear_aggregators(nombre='accumulator_Q3', cantidad=1, entrada='Queue_between_aggregators_acumulator_Q3',
                          salida='Queue_between_reducer_join_Q3', type='accumulator',
                          params='store_id,tpv,year_semester'))
    services.update(crear_aggregators(nombre='Counter_Q4', cantidad=cant_nodos, entrada='Queue_begin_4',
                                      salida='Queue_between_aggregator_reducer_Q4', type='counter',
                                      params='store_id,user_id,purchases_qty'))
    services.update(crear_reducers(nombre='Reducer_Q21', entrada='Queue_between_aggregator_reducer_Q21',
                                   salida='Queue_between_reducer_joiner_Q21', top=1,
                                   params='year_month,item_id,total_quantity'))
    services.update(crear_reducers(nombre='Reducer_Q22', entrada='Queue_between_aggregator_reducer_Q22',
                                   salida='Queue_between_reducer_joiner_Q22', top=1,
                                   params='year_month,item_id,total_earnings'))
    services.update(crear_reducers(nombre='Reducer_Q4', entrada='Queue_between_aggregator_reducer_Q4',
                                   salida='Queue_between_reducer_joiner_Q4', top=3,
                                   params='store_id,user_id,purchases_qty'))
    services.update(crear_filters(nombre='Filter_amount_Q1', cantidad=cant_nodos, entrada='Queue_1',
                                  salida='Queue_between_filter_amount_filter_columna_Q1', type='byamount'))
    services.update(crear_filters(nombre='Filter_column_Q1', cantidad=cant_nodos,
                                  entrada='Queue_between_filter_amount_filter_columna_Q1',
                                  salida='Queue_final_Q1', type='bycolumn'))
    services.update(
        crear_joiners(nombre='Join_productos_Q21', cantidad=cant_nodos, entradaJoin='exchange,productsExchange',
                      entradaData='Queue_between_reducer_joiner_Q21', salida='Queue_final_Q21',
                      disk_type=False, params='item_name,item_id'))
    services.update(
        crear_joiners(nombre='Join_productos_Q22', cantidad=cant_nodos, entradaJoin='exchange,productsExchange',
                      entradaData='Queue_between_reducer_joiner_Q22', salida='Queue_final_Q22',
                      disk_type=False, params='item_name,item_id'))
    services.update(crear_joiners(nombre='Join_stores_Q3', cantidad=cant_nodos, entradaJoin='exchange,storesExchange',
                                  entradaData='Queue_between_aggregators_acumulator_Q3', salida='Queue_final_Q3',
                                  disk_type=False, params='store_name,store_id'))
    services.update(crear_joiners(nombre='Join_users_Q4', cantidad=cant_nodos, entradaJoin='exchange,usersExchange',
                                  entradaData='Queue_between_reducer_joiner_Q4',
                                  salida='Queue_between_joiner_users_and_joiner_stores_Q4',
                                  disk_type=True, params='birthdate,user_id'))
    services.update(crear_joiners(nombre='Join_stores_Q4', cantidad=cant_nodos, entradaJoin='exchange,storesExchange',
                                  entradaData='Queue_between_joiner_users_and_joiner_stores_Q4',
                                  salida='Queue_final_Q4',
                                  disk_type=False, params='store_name,store_id'))

    services.update(crear_client(cant_clientes, 5000))
    data = {'services': services,
            'networks': {
                'mynet': {
                    'external': False,
                    'driver': 'bridge',
                }
            }
            }

    yaml.dump(data, f, sort_keys=False, default_flow_style=False, line_break='\n', explicit_start=True)
