import subprocess
import random
import time

# CONTAINER_NAMES = ["filtroanio2_1","filtrohora1_1","filter_amount_q1_1","filter_column_q1_1","aggregator_suma_q3_1"]
CONTAINER_NAMES = [
'aggregator_accumulator_q21_1', 'aggregator_accumulator_q22_1', 'aggregator_accumulator_q1_1', 'aggregator_accumulator_q3_1', 'aggregator_accumulator_q4_1',
'filtroanio1_1','filtroanio1_4','filtroanio2_2','filtroanio2_5','filtrohora1_3','filter_amount_q1_1','filter_amount_q1_4','filter_column_q1_2','filter_column_q1_5',
'aggregator_suma_q21_3','aggregator_suma_q22_1','aggregator_suma_q22_4','reducer_reducer_q22','aggregator_suma_q3_2','aggregator_suma_q3_5','aggregator_counter_q4_1',
'aggregator_counter_q4_4','reducer_reducer_q4','join_users_q4_2','join_stores_q4_1','healthchecker_2', 'filtroanio1_2','filtroanio1_5','filtroanio2_3','filtrohora1_1',
'filtrohora1_4','filter_amount_q1_2','filter_amount_q1_5','filter_column_q1_3','aggregator_suma_q21_1','aggregator_suma_q21_4','reducer_reducer_q21','aggregator_suma_q22_2',
'aggregator_suma_q22_5','join_productos_q22_1','aggregator_suma_q3_3','aggregator_counter_q4_2','aggregator_counter_q4_5','join_users_q4_1','join_users_q4_3','healthchecker_3', 
'filtroanio1_3','filtroanio2_1','filtroanio2_4','filtrohora1_2','filtrohora1_5','filter_amount_q1_3','filter_column_q1_1','filter_column_q1_4','aggregator_suma_q21_2','aggregator_suma_q21_5',
'join_productos_q21_1','aggregator_suma_q22_3','aggregator_suma_q3_1','aggregator_suma_q3_4','join_stores_q3_1','aggregator_counter_q4_3','join_users_q4_5','join_users_q4_4','healthchecker_1'
]

SECONDS = 15
last_container_name = None

def run():
    global last_container_name
    while True:
        container = random.choice(CONTAINER_NAMES)
        if container == last_container_name:
            i = CONTAINER_NAMES.index(container) + 1
            if i >= len(CONTAINER_NAMES):
                i = 0
            container = CONTAINER_NAMES[i]

        result = subprocess.run(['docker', 'kill', container], check=False, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        print(f'apuñale a {container}. me gusta verlo desangrarse, jeje.')
        last_container_name = container
        if result.returncode != 0:
            print(f"Error al matar a {container}. Tenia poderes divinos?")
            continue
        time.sleep(SECONDS)


if __name__ == "__main__":
    run()
