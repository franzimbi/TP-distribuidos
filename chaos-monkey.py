import subprocess
import random
import time

# CONTAINER_1 = ['filtroanio1_3', 'filtroanio2_1', 'filtroanio2_6', 'filtrohora1_4', 'filter_amount_q1_2',
#                'filter_amount_q1_7', 'filter_column_q1_5', 'aggregator_suma_q21_3',
#                'aggregator_suma_q22_3', 'aggregator_suma_q3_3',
#                 'aggregator_counter_q4_4', 'reducer_reducer_q4', 'join_users_q4_4',
#                'healthchecker_4','filtroanio1_2', 'filtroanio1_7', 'filtroanio2_5', 'filtrohora1_3',
#                'filter_amount_q1_1','filter_amount_q1_6', 'filter_column_q1_4', 'aggregator_suma_q21_2', 'aggregator_suma_q21_7',
#                'aggregator_suma_q22_2', 'aggregator_suma_q22_7', 'aggregator_accumulator_q3_1', 'aggregator_accumulator_q22_1']
# CONTAINER_2 = ['filtroanio1_4', 'filtroanio2_2', 'filtroanio2_7', 'filtrohora1_5', 'filter_amount_q1_3',
#                'filter_column_q1_1', 'filter_column_q1_6', 'aggregator_suma_q21_4', 'reducer_reducer_q21',
#                'aggregator_suma_q22_4', 'reducer_reducer_q22', 'aggregator_suma_q3_4', 'join_stores_q3_1',
#                'aggregator_counter_q4_5', 'join_users_q4_1', 'join_stores_q4_1',
#                'filtroanio1_3', 'filtroanio2_1', 'filtroanio2_6', 'filtrohora1_4', 'filter_amount_q1_2', 'filter_amount_q1_7',
#                'filter_column_q1_5', 'aggregator_suma_q21_3', 'aggregator_suma_q22_3',
#                'aggregator_suma_q3_3', 'aggregator_counter_q4_4', 'aggregator_accumulator_q4_1']
# CONTAINER_3 = ['filtroanio1_5', 'filtroanio2_3', 'filtrohora1_1', 'filtrohora1_6', 'filter_amount_q1_4',
#                'filter_column_q1_2', 'filter_column_q1_7', 'aggregator_suma_q21_5', 'join_productos_q21_1',
#                'aggregator_suma_q22_5', 'join_productos_q22_1', 'aggregator_suma_q3_5', 'aggregator_counter_q4_1',
#                'aggregator_counter_q4_6', 'join_users_q4_5', 'healthchecker_1','aggregator_suma_q3_2',
#                'aggregator_suma_q3_7','aggregator_counter_q4_3', 'join_users_q4_3', 'healthchecker_3',
#                'reducer_reducer_q4', 'join_users_q4_4', 'healthchecker_4', 'aggregator_accumulator_q21_1']

CONTAINER_1= ['aggregator_accumulator_q21_1', 'aggregator_accumulator_q22_1', 'aggregator_accumulator_q3_1']
CONTAINER_2= ['aggregator_accumulator_q4_1']
CONTAINER_3= ['aggregator_accumulator_q21_1', 'aggregator_accumulator_q22_1', 'aggregator_accumulator_q3_1']
SECONDS = 20

def run():
    while True:
        containers = [random.choice(CONTAINER_1), random.choice(CONTAINER_3), random.choice(CONTAINER_2)]

        special = [c for c in containers if c.startswith("aggregator_accumulator_")]

        if special:
            for c in special:
                subprocess.run(['docker', 'kill', c], check=False, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
                print(f'apuñale a {c}. me gusta verlo desangrarse, jeje.')
        else:
            for n in containers:
                subprocess.run(['docker', 'kill', n], check=False, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
                print(f'apuñale a {n}. me gusta verlo desangrarse, jeje.')

        print("---------------------------------------------------------")
        time.sleep(SECONDS)


if __name__ == "__main__":
    run()
