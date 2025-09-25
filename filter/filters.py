from datetime import datetime, time
from common.batch import Batch

def filter_by_time(batch: Batch):
    #filtered = []
    filtered = Batch(type_file=batch._type_file)
    for row in batch._body:
        try:
            dt = datetime.strptime(row[8], "%Y-%m-%d %H:%M:%S")
            if time(6,0,0) <= dt.time() <= time(22,59,59):
                filtered.add_row(row)
        except Exception:
            # Si la fecha no se puede parsear, descartar
            print("no pude parsear la fecha")
            continue
    return filtered


def filter_by_amount(batch: Batch):
    #filtered = []
    filtered = Batch(type_file=batch._type_file)
    for row in batch._body:
        try:
            if float(row[7]) >= 75:
                filtered.add_row(row)
        except Exception:
            print("no pude parsear la cantidad")
            continue
    return filtered


def filter_by_column(batch: Batch):
    #filtered = []
    filtered = Batch(type_file=batch._type_file)
    for row in batch._body:
        aux = [row[0], row[7]]
        filtered.add_row(row)
    return filtered
