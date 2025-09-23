from middleware.middleware import MessageMiddlewareQueue

def make_batches_from_csv(path, batch_size, queue: MessageMiddlewareQueue):
    current_batch = []

    with open(path, 'r') as f:
        next(f)
        for line in f:
            current_batch.append(line.strip())
            if len(current_batch) >= batch_size:
                queue.send(encode_batch(current_batch))
                current_batch = []
    if current_batch:
        queue.send(encode_batch(current_batch))

    queue.send(b"&END&")

def encode_batch(batch):
    batch_str = "|".join(batch)
    return batch_str.encode("utf-8")

def decode_batch(data):
    batch_str = data.decode("utf-8")
    return [line.split(",") for line in batch_str.strip().split("|")]