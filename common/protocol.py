# from middleware.middleware import MessageMiddlewareQueue
import os


# def make_batches_from_csv(path, batch_size, queue: MessageMiddlewareQueue):
#     for filename in os.listdir(path):
#         current_batch = []
#
#         with open(path + '/' + filename, 'r') as f:
#             next(f)
#             for line in f:
#                 current_batch.append(line.strip())
#                 if len(current_batch) >= batch_size:
#                     queue.send(encode_batch(current_batch))
#                     current_batch = []
#         if current_batch:
#             queue.send(encode_batch(current_batch))
#
#     queue.send(b"&END&")
#
#
# def encode_batch(batch):
#     batch_str = "|".join(batch)
#     return batch_str.encode("utf-8")
#
#
# def decode_batch(data):
#     batch_str = data.decode("utf-8")
#     return [line.split(",") for line in batch_str.strip().split("|")]


class Batch:
    """ id: identificador de batches.
        last_batch: flag para identificar si es el ultimo batch
        type_file: define el tipo de csv:
                                        - i: items_menu
                                        - m: payment_method
                                        - s: stores
                                        - r: transaction_items
                                        - t: transactions
                                        - u: users
                                        - v: vouchers
        header: nombres de las columnas del batch
        rows: filas del batch
        size: size de batch
        """

    def __init__(self, id: int, last=False, type_file='', header=None, rows=None):
        self._id_batch = int(id)
        self._last_batch = last
        self._type_file = type_file
        self._header = header if header is not None else []
        self._body = rows if rows is not None else []
        self._size = len(self._body)

    def __eq__(self, other):
        if not isinstance(other, Batch):
            return NotImplemented
        return (
                self._id_batch == other._id_batch
        )

    def id(self):
        return self._id_batch

    def is_last_batch(self):
        return self._last_batch

    def get_header(self):
        return self._header

    def index_of(self, header_name: str):
        try:
            i = self._header.index(header_name)
            return i
        except ValueError:
            return None

    def iter_per_header(self):
        """podes iterar las filas del batch y buscar por nombre de columna.
        ejempo:
            for i in batch.iter_per_header()
                print(i['columna_1'])
            salida:
                f1c1
                f2c1
                f3c1
                ...
            """

        for row in self._body:
            yield dict(zip(self._header, row))

    def __getitem__(self, idx):
        """
        podes hacer:
        - batch[i] -> fila i (lista)
        - batch[i, j] -> fila i, columna j (j es un int)
        - batch[i, 'col_name'] -> fila i columna 'col_name' segun headers
        """
        if isinstance(idx, tuple) and len(idx) == 2:  # tiene doble indice
            row_idx, col_idx = idx
            row = self._body[row_idx]

            if isinstance(col_idx, int):
                return row[col_idx]
            elif isinstance(col_idx, str):
                try:
                    col_pos = self._header.index(col_idx)
                    return row[col_pos]
                except ValueError:
                    return None
            else:
                raise TypeError("indices mal")
        else:
            return self._body[idx]

    def __len__(self):
        return len(self._body)

    def __iter__(self):
        """Permite iterar sobre las filas del body"""
        return iter(self._body)

    def encode(self) -> bytes:
        res = b''
        res += self._id_batch.to_bytes(4, byteorder='big', signed=False)
        if self._last_batch:
            res += (1).to_bytes(1, byteorder="big", signed=False)
        else:
            res += (0).to_bytes(1, byteorder="big", signed=False)
        res += self._type_file.encode("utf-8")
        header_joined = ",".join(self._header).encode("utf-8")
        res += len(header_joined).to_bytes(4, byteorder="big", signed=False)
        res += header_joined
        res += self._size.to_bytes(4, byteorder='big', signed=False)

        body_joined = "|".join([",".join(l) for l in self._body])
        body_joined = body_joined.encode("utf-8")

        res += len(body_joined).to_bytes(4, byteorder="big", signed=False)
        res += body_joined

        return res

    def decode(self, data: bytes):
        offset = 0
        self._id_batch = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4
        self._last_batch = bool(int.from_bytes(data[offset:offset + 1], byteorder='big', signed=False))
        offset += 1
        self._type_file = data[offset:offset + 1].decode("utf-8")
        offset += 1
        header_len = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4
        header_bytes = data[offset:offset + header_len]
        self._header = header_bytes.decode("utf-8").split(",") if header_len > 0 else []
        offset += header_len

        self._size = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4

        body_len = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4

        body_bytes = data[offset:offset + body_len]
        body_str = body_bytes.decode("utf-8")
        self._body = [line.split(",") for line in body_str.split("|")] if body_len > 0 else []

    def __str__(self):
        lines = [
            f"Batch ID: {self._id_batch}",
            f"Last batch: {self._last_batch}",
            f"Type file: {self._type_file}",
            f"Header: {self._header}",
            f"Size: {self._size}",
            "Body:"
        ]
        max_rows = 10
        for i, row in enumerate(self._body[:max_rows]):
            lines.append(f"  {i + 1}: {row}")

        if self._size > max_rows:
            lines.append(f"  ... y {self._size - max_rows} filas más ...")

        return "\n".join(lines)

    def delete_column(self, col):
        """
        borra una columna:
        - col puede ser índice (int) o nombre de columna (str)
        """
        if isinstance(col, str):
            try:
                col_idx = self._header.index(col)
            except ValueError:
                return
        elif isinstance(col, int):
            col_idx = col
        else:
            raise TypeError("col debe ser int o str")

        # elimina columna del header
        del self._header[col_idx]

        # eliminr columna de cada fila
        for row in self._body:
            if col_idx < len(row):
                del row[col_idx]

    def delete_row(self, row_idx):
        """
        borra una fila por índice
        """
        if 0 <= row_idx < len(self._body):
            del self._body[row_idx]
            self._size -= 1

# aux = Batch(5, False, 't', ['a', 'b', 'c'], [['1', '2', '3'], ['fw3', 'efw', 'ewq'], ['123e', '2w', '3r']])
#
#
# print('for comun\n')
# for i in aux:
#     print(i)
# print('for por header')
# for i in aux.iter_per_header():
#     print(i)
# print('len: ' + str(len(aux)))
# print('get_item aux[0]:' + str(aux[0]))
# print('get_item aux[0][2]:' + str(aux[0][2]))
# print('get_item aux[1, a]:' + str(aux[1, 'a']))
# print('index_of:' + str(aux.index_of('b')))
# print('id:' + str(aux.id()))
# print('is last:' + str(aux.is_last_batch()))


# for comun
#
# ['1', '2', '3']
# ['fw3', 'efw', 'ewq']
# ['123e', '2w', '3r']
# for por header
# {'a': '1', 'b': '2', 'c': '3'}
# {'a': 'fw3', 'b': 'efw', 'c': 'ewq'}
# {'a': '123e', 'b': '2w', 'c': '3r'}
# len: 3
# get_item aux[0]:['1', '2', '3']
# get_item aux[0][2]:3
# get_item aux[1, a]:fw3
# index_of:1
# id:5
# is last:False

# aux.delete_row(0)
# print(aux)
# aux.delete_column(0)
# print(aux)
# aux.delete_column('b')
# print(aux)