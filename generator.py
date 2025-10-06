#!/usr/bin/env python3
import sys
import yaml

if len(sys.argv) != 3:
    print("Uso: python3 generador.py <cantidad nodos> <cantidad clientes>")
    sys.exit(1)

cant_nodos = int(sys.argv[1])
cant_clientes = int(sys.argv[2])
nombre_file = 'docker-compose-dev.yaml'


def crear_distributor():
    data = {
        'build': {
            'context': '.',
            'dockerfile': 'distributor/Dockerfile',
        },
        'restart': 'on-failure',
        'environment': [
            'PYTHONUNBUFFERED = 1',
            'Q1result=Queue_final_Q1',
            'Q21result=Queue_final_Q21',
            'Q22result=Queue_final_Q22',
            'Q3result=Queue_final_Q3',
            'Q4result=Queue_final_Q4',
            'transactionsQueue=transactionsQueue',
            'itemsQueue=itemsQueue',
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
        filter_name = f'{nombre}_{i}'
        conection_coordinator = f'_coordinator_produce_for_{filter_name}'
        if i == 1:
            queues_to_coordinator += f'{conection_coordinator}'
        else:
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
                'CONSUME_QUEUE=' + entrada,
                # 'CONSUME_QUEUE=entradaFilter' + nombre + '_' + str(i),
                # 'tipoSalida=queue',
                'PRODUCE_QUEUE=' + salida,
                'QUEUE_PRODUCE_FOR_COORDINATOR=coodinator_' + str(nombre) + '_' + 'unique_queue',
                'QUEUE_CONSUME_FROM_COORDINATOR=' + conection_coordinator,
                'FILTER_NAME=' + type
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
            'dockerfile': 'coordinator/Dockerfile',
        },
        'restart': 'on-failure',
        'environment': [
            'PYTHONUNBUFFERED=1',
            'QUEUE_CONSUME_FROM_NODES=_coordinator_consumes_from_'+ str(nombre) + '_' ,
            'NUM_NODES=' + str(cantidad),
            'QUEUES_PRODUCE_FOR_NODES=' + queues_to_coordinator,
            # 'tipoSalida=queue',
            'DOWNSTREAM_QUEUE=' + salida,
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
    aggregators = {}
    # queues_to_coordinator = ''
    for i in range(1, cantidad + 1):
        aggregator_name = f'aggregator{nombre}_{i}'
        aggregators[aggregator_name] = {
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
                'type=' + type,
                'params=' + params,
            ],
            'networks': [
                'mynet'
            ],
            'volumes': [
                './config.ini:/app/config.ini'
            ],
        }
    return aggregators


def crear_reducers(nombre, cantidad, entrada, salida, top, params):
    reducers = {}
    # queues_to_coordinator = ''
    for i in range(1, cantidad + 1):
        reducer_name = f'reducer{nombre}_{i}'
        reducers[reducer_name] = {
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
                'queueEntrada=' + entrada,
                'queuesSalida=' + salida,
                'top=' + str(top),
                'params=' + params
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
    queues_to_coordinator = ''
    for i in range(1, cantidad + 1):
        joiner_name = f'{nombre}_{i}'
        conection_coordinator = f'{joiner_name}_receive_from_coordinator_{nombre}'
        if i == 1:
            queues_to_coordinator += f'{conection_coordinator}'
        else:
            queues_to_coordinator += f',{conection_coordinator}'
        joiners[joiner_name] = {
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
                # 'tipoSalida=queue',
                'queuesSalida=' + salida,
                'queue_to_send_coordinator=coodinator_' + str(nombre) + '_' + 'unique_queue',
                'queue_to_receive_coordinator=' + conection_coordinator,
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

    coordinator_name = f'coordinator_{nombre}'
    joiners[coordinator_name] = {
        'build': {
            'context': '.',
            'dockerfile': 'coordinator/Dockerfile',
        },
        'restart': 'on-failure',
        'environment': [
            'PYTHONUNBUFFERED=1',
            'queue_entrada=coodinator_' + str(nombre) + '_' + 'unique_queue',
            'NUM_NODES=' + str(cantidad),
            'queues_to_send_to_nodes=' + queues_to_coordinator,
            # 'tipoSalida=queue',
            'DOWNSTREAM_QUEUE=' + salida,
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
    #             equivalente al viejo:
    #                               - AGGREGATOR_NAME=sum               type
    #                               - KEY_COLUMN=item_id                params[0]
    #                               - VALUE_COLUMN=quantity             params[1]
    #                               - BUCKET_KIND=month                 params[2]
    #                               - BUCKET_NAME=year_month            params[3]
    #                               - TIME_COL=created_at               params[4]
    #                               - OUT_VALUE=total_quantity          params[5]

    services.update(crear_aggregators(nombre='Suma_Q22', cantidad=1, entrada='Queue_begin2_2',
                                      salida='Queue_between_aggregator_reducer_Q22', type='sum',
                                      params='earnings,item_id,subtotal,month,year_month,created_at,total_earnings'))
    services.update(crear_aggregators(nombre='Suma_Q3', cantidad=1, entrada='Queue_3',
                                      salida='Queue_between_aggregator_join_Q3', type='sum',
                                      params='store_id,final_amount,semester,year_semester,created_at,tpv'))
    #               equivalente al viejo:
    #                       - AGGREGATOR_NAME=sum               type
    #                       - KEY_COLUMN=store_id               params[0]
    #                       - VALUE_COLUMN=final_amount         params[1]
    #                       - BUCKET_KIND=semester              params[2]
    #                       - BUCKET_NAME=year_semester         params[3]
    #                       - TIME_COL=created_at               params[4]
    #                       - OUT_VALUE=tpv                     params[5]
    services.update(crear_aggregators(nombre='Counter_Q4', cantidad=1, entrada='Queue_begin_4',
                                      salida='Queue_between_aggregator_reducer_Q4', type='counter',
                                      params='store_id,user_id,purchases_qty'))
    #               equivalente al viejo:
    #                       - KEY_COLUMNS=store_id,user_id
    #                       - COUNT_NAME=purchases_qty
    #                       - AGGREGATOR_NAME=counter
    services.update(crear_reducers(nombre='Reducer_Q21', cantidad=1, entrada='Queue_between_aggregator_reducer_Q21',
                                   salida='Queue_between_reducer_joiner_Q21', top=1,
                                   params='year_month,item_id,total_quantity'))
    services.update(crear_reducers(nombre='Reducer_Q22', cantidad=1, entrada='Queue_between_aggregator_reducer_Q22',
                                   salida='Queue_between_reducer_joiner_Q22', top=1,
                                   params='year_month,item_id,total_earnings'))

    services.update(crear_reducers(nombre='Reducer_Q4', cantidad=1, entrada='Queue_between_aggregator_reducer_Q4',
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
    #                   equivalente al viejo:
    #                           - COLUMN_NAME=item_name
    #                           - COLUMN_ID=item_id
    services.update(
        crear_joiners(nombre='Join_productos_Q22', cantidad=cant_nodos, entradaJoin='exchange,productsExchange',
                      entradaData='Queue_between_reducer_joiner_Q22', salida='Queue_final_Q22',
                      disk_type=False, params='item_name,item_id'))
    services.update(crear_joiners(nombre='Join_stores_Q3', cantidad=cant_nodos, entradaJoin='exchange,storesExchange',
                                  entradaData='Queue_between_aggregator_join_Q3', salida='Queue_final_Q3',
                                  disk_type=False, params='store_name,store_id'))
    services.update(crear_joiners(nombre='Join_users_Q4', cantidad=cant_nodos, entradaJoin='exchange,usersExchange',
                                  entradaData='Queue_between_reducer_joiner_Q4',
                                  salida='Queue_between_joiner_users_and_joiner_stores_Q4',
                                  disk_type=True, params='item_name,item_id'))
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
