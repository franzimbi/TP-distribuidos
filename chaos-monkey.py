import subprocess
import random
import time

# CONTAINER_NAMES = [
#     "filtroanio2_1_1","filtrohora1_1_1","filter_amount_q1_1_1","filter_column_q1_1_1"] filtrohora1_1
CONTAINER_NAMES = [
    "filtrohora1_1"]
SECONDS = 300
def run():
    while True:
        container = random.choice(CONTAINER_NAMES)
        result = subprocess.run(['docker', 'kill', container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f'apuñale a {container}. me gusta verlo desangrarse, jeje.')
        if result.returncode != 0:
            print(f"Error al matar a {container}. Tenia poderes divinos? Stderr: {result.stderr.decode('utf-8')}")
            return
        time.sleep(SECONDS)

if __name__ == "__main__":
    run()