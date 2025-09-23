from datetime import datetime, time


def filter_by_time(batch: list):
    filtered = []
    for row in batch:
        try:
            dt = datetime.strptime(row[8], "%Y-%m-%d %H:%M:%S")
            if time(6) <= dt.time() <= time(23):
                filtered.append(row)
        except Exception:
            # Si la fecha no se puede parsear, descartar
            continue
    return filtered


def filter_by_amount(batch: list):
    filtered = []
    for row in batch:
        try:
            if float(row[7]) >= 75:
                filtered.append(row)
        except Exception:
            continue
    return filtered


def filter_by_column(batch: list):
    filtered = []
    for row in batch:
        aux = [row[0], row[7]]
        filtered.append(aux)
    return filtered
