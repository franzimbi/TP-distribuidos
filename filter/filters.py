from datetime import datetime, time
from common.batch import Batch


def filter_by_time(batch: Batch):
    index = batch.index_of('created_at')
    if index is None:
        raise ValueError("El batch no tiene la columna 'created_at'")
    filtered = []
    for row in batch:
        try:
            dt = datetime.strptime(row[index], "%Y-%m-%d %H:%M:%S")
            if time(6, 0, 0) <= dt.time() <= time(22, 59, 59):
                filtered.append(row)
        except Exception:
            # Si la fecha no se puede parsear, descartar
            print("no pude parsear la fecha")
            continue

    batch.replace_all_rows(filtered)
    return batch


def filter_by_amount(batch: Batch):
    index = batch.index_of('final_amount')
    filtered = []
    for row in batch:
        try:
            if float(row[index]) >= 75:
                filtered.append(row)
        except ValueError:
            continue
    batch.replace_all_rows(filtered)
    return batch


def filter_by_column(batch: Batch):
    keep = ['transaction_id', 'final_amount']
    for col in list(batch.get_header()):  # list() para evitar problemas al modificar _header mientras iterás
        if col not in keep:
            batch.delete_column(col)
    return batch

