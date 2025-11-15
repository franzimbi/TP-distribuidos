#!/usr/bin/env python3
import sys
import yaml

if len(sys.argv) != 6:
    print("Uso: python3 generador.py <cantidad filtros> <cantidad aggregators> <cantidad joins> <cantidad clientes> <csv_input_dir>")
    sys.exit(1)

cant_filtros = int(sys.argv[1])
cant_aggregators = int(sys.argv[2])
cant_joins = int(sys.argv[3])
cant_clientes = int(sys.argv[4])
csv_input_dir = sys.argv[5]
nombre_file = 'docker-compose-dev.yaml'


def crear_distributor(cantidad_joiners):
    usr_queues = ''
    for i in range(1, cantidad_joiners + 1):
        if i != 1:
            usr_queues += ','
        usr_queues += f'join_users_queue_{i}'
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
            'transactionsQueue=transactionsQueue',
            'itemsQueue=itemsQueue',
            'productsQueues=join_products_queue_q22_1,join_products_queue_q21_1',
            'storesQueues=join_stores_queue_q3_1,join_stores_queue_q4_1',
            'CONFIRMATION_QUEUE=q_joins_confirmation',
            f'usersQueues={usr_queues}',
            'numberOfJoins=' + str(cant_joins+4),
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
            'container_name': f'{nombre}_{i}'.lower(),
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
        aggregator_name = f'aggregator_{nombre}_{i}'
        aggregators[aggregator_name.lower()] = {
            'container_name': f'aggregator_{nombre}_{i}'.lower(),
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
    reducer_name = f'reducer_{nombre}'
    reducers[reducer_name.lower()] = {
        'container_name': f'reducer_{nombre}',
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


def crear_joiners(nombre, cantidad, entrada, salida, entrada_join, params):
    joiners = {}
    if cantidad > 1:
        salida_j1 = 'conection_join_1'
        is_last_j1 = 'False'
    else:
        salida_j1 = salida
        is_last_j1 = 'True'
    joiners[f'{nombre}_{1}'.lower()] = {
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
            'queueEntradaJoin=' + entrada_join + '_1',
            'queueEntradaData=' + entrada,
            'queuesSalida=' + salida_j1,
            'is_last_join=' + is_last_j1,
            'CONFIRMATION_QUEUE=q_joins_confirmation',
            'params=' + params
        ],
        'networks': [
            'mynet'
        ],
        'volumes': [
            './config.ini:/app/config.ini'
        ],
    }

    if cantidad > 1:
        entrada_j2 = f'conection_join_{int(cantidad) - 1}'
    else:
        return joiners
    joiners[f'{nombre}_{cantidad}'.lower()] = {
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
            f'queueEntradaJoin=' + entrada_join + f'_{cantidad}',
            'queueEntradaData=' + entrada_j2,
            'queuesSalida=' + salida,
            'is_last_join=' + 'True',
            'CONFIRMATION_QUEUE=q_joins_confirmation',
            'params=' + params
        ],
        'networks': [
            'mynet'
        ],
        'volumes': [
            './config.ini:/app/config.ini'
        ],
    }

    if cantidad > 2:
        for i in range(2, int(cantidad)):
            joiner_name = f'{nombre}_{i}'
            entrada_join = f'join_users_queue_{i}'
            join_entrada_data = f'conection_join_{i - 1}'
            join_salida_data = f'conection_join_{i}'

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
                    'queueEntradaJoin=' + entrada_join,
                    'queueEntradaData=' + join_entrada_data,
                    'queuesSalida=' + join_salida_data,
                    'is_last_join=' + 'False',
                    'CONFIRMATION_QUEUE=q_joins_confirmation',
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

def crear_client(cantidad, puerto, input_dir):
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
                'CSV_INPUT_DIR=' + input_dir,
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

def crear_healthcheckers(cantidad, puerto):


with open(nombre_file, 'w') as f:
    services = {'distributor': crear_distributor(cant_joins)}
    # nodos de entrada a todas las queries
    services.update(crear_filters(nombre='FiltroAnio1', cantidad=cant_filtros, entrada='itemsQueue',
                                  salida='Queue_begin2_1,Queue_begin2_2', type='byyear'))
    services.update(crear_filters(nombre='FiltroAnio2', cantidad=cant_filtros, entrada='transactionsQueue',
                                  salida='Queue_begin_4,Queue_begin_3_y_1', type='byyear'))
    services.update(crear_filters(nombre='FiltroHora1', cantidad=cant_filtros, entrada='Queue_begin_3_y_1',
                                  salida='Queue_3,Queue_1', type='bytime'))

    # querie 1
    services.update(crear_filters(nombre='Filter_amount_Q1', cantidad=cant_filtros, entrada='Queue_1',
                                  salida='Queue_between_filter_amount_filter_columna_Q1', type='byamount'))

    services.update(crear_filters(nombre='Filter_column_Q1', cantidad=cant_filtros,
                                  entrada='Queue_between_filter_amount_filter_columna_Q1',
                                  salida='Queue_final_Q1', type='bycolumn'))

    # querie 21
    services.update(crear_aggregators(nombre='Suma_Q21', cantidad=cant_aggregators, entrada='Queue_begin2_1',
                                      salida='Queue_between_aggregator_accumulator_Q21', type='sum',
                                      params='item_id,quantity,month,year_month,created_at,total_quantity'))

    services.update(
        crear_aggregators(nombre='accumulator_Q21', cantidad=1, entrada='Queue_between_aggregator_accumulator_Q21',
                          salida='Queue_between_accumulator_reducer_Q21', type='accumulator',
                          params='item_id,total_quantity,year_month'))

    services.update(crear_reducers(nombre='Reducer_Q21', entrada='Queue_between_accumulator_reducer_Q21',
                                   salida='Queue_between_reducer_joiner_Q21', top=1,
                                   params='year_month,item_id,total_quantity'))
    services.update(
        crear_joiners(nombre='Join_productos_Q21', cantidad=1,
                      entrada='Queue_between_reducer_joiner_Q21',
                      salida='Queue_final_Q21',
                      entrada_join='join_products_queue_q21',
                      params='item_name,item_id'))

    # querie 22
    services.update(crear_aggregators(nombre='Suma_Q22', cantidad=cant_aggregators, entrada='Queue_begin2_2',
                                      salida='Queue_between_aggregator_accumulator_Q22', type='sum',
                                      params='item_id,subtotal,month,year_month,created_at,total_earnings'))

    services.update(
        crear_aggregators(nombre='accumulator_Q22', cantidad=1, entrada='Queue_between_aggregator_accumulator_Q22',
                          salida='Queue_between_accumulator_reducer_Q22', type='accumulator',
                          params='item_id,total_earnings,year_month'))

    services.update(crear_reducers(nombre='Reducer_Q22', entrada='Queue_between_accumulator_reducer_Q22',
                                   salida='Queue_between_reducer_joiner_Q22', top=1,
                                   params='year_month,item_id,total_earnings'))

    services.update(
        crear_joiners(nombre='Join_productos_Q22', cantidad=1,
                      entrada='Queue_between_reducer_joiner_Q22',
                      salida='Queue_final_Q22',
                      entrada_join='join_products_queue_q22',
                      params='item_name,item_id'))

    # querie 3
    services.update(crear_aggregators(nombre='Suma_Q3', cantidad=cant_aggregators, entrada='Queue_3',
                                      salida='Queue_between_aggregators_accumulator_Q3', type='sum',
                                      params='store_id,final_amount,semester,year_semester,created_at,tpv'))
    services.update(
        crear_aggregators(nombre='accumulator_Q3', cantidad=1, entrada='Queue_between_aggregators_accumulator_Q3',
                          salida='Queue_between_accumulator_join_Q3', type='accumulator',
                          params='store_id,tpv,year_semester'))

    services.update(
        crear_joiners(nombre='Join_stores_Q3', cantidad=1,
                      entrada='Queue_between_accumulator_join_Q3',
                      salida='Queue_final_Q3',
                      entrada_join='join_stores_queue_q3',
                      params='store_name,store_id'))

    # querie 4
    services.update(crear_aggregators(nombre='Counter_Q4', cantidad=cant_aggregators, entrada='Queue_begin_4',
                                      salida='Queue_between_aggregators_accumulator_Q4', type='counter',
                                      params='store_id,user_id,purchases_qty'))

    services.update(
        crear_aggregators(nombre='accumulator_Q4', cantidad=1, entrada='Queue_between_aggregators_accumulator_Q4',
                          salida='Queue_between_accumulator_reducer_Q4', type='accumulator',
                          params='store_id,purchases_qty,user_id'))

    services.update(crear_reducers(nombre='Reducer_Q4', entrada='Queue_between_accumulator_reducer_Q4',
                                   salida='Queue_between_reducer_joiner_Q4', top=3,
                                   params='store_id,user_id,purchases_qty'))

    services.update(
        crear_joiners(nombre='Join_users_Q4', cantidad=cant_joins,
                      entrada='Queue_between_reducer_joiner_Q4',
                      salida='Queue_between_joiner_users_and_joiner_stores_Q4',
                      entrada_join='join_users_queue',
                      params='birthdate,user_id'))
    services.update(crear_joiners(nombre='Join_stores_Q4', cantidad=1,
                                  entrada='Queue_between_joiner_users_and_joiner_stores_Q4',
                                  salida='Queue_final_Q4',
                                  entrada_join='join_stores_queue_q4',
                                  params='store_name,store_id'))

    # clientes
    services.update(crear_client(cant_clientes, 5000, csv_input_dir))
    
    # red
    data = {'services': services,
            'networks': {
                'mynet': {
                    'external': False,
                    'driver': 'bridge',
                }
            }
            }

    yaml.dump(data, f, sort_keys=False, default_flow_style=False, line_break='\n', explicit_start=True)
