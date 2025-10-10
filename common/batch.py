import copy


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

    def __init__(self, id=0, query_id=0, client_id=0, last=False, type_file=' ', header=None, rows=None):
        self._id_batch = int(id)
        self._query_id = int(query_id)
        self._client_id = int(client_id)
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

    def client_id(self):
        return self._client_id

    def type(self):
        return self._type_file

    def set_id(self, id: int):
        self._id_batch = int(id)

    def change_header_name_value(self, old_column, new_column, dictionary):
        try:
            id = self._header.index(old_column)
            self._header[id] = new_column
        except ValueError as e:
            raise ValueError(f'la columna {old_column} no existe en el header')
        body_copy = copy.deepcopy(self._body)
        for i in body_copy:
            try:
                i[id] = dictionary[i[id]]
            except KeyError as e:
                self._header[self._header.index(new_column)] = old_column
                raise KeyError(f'el id {i[id]} no existe en el dictionary')
        self._body = body_copy

    def set_header(self, header):
        """
        setea los headers del batch.
        - header puede ser:
            * string CSV (ej: "id,nombre,edad")
            * lista de strings (ej: ["id","nombre","edad"])
        Solo se permite si el batch está vacío (sin filas).
        """
        if isinstance(header, str):
            header_list = header.strip().split(",") if header.strip() else []
        elif isinstance(header, list):
            header_list = header
        else:
            raise TypeError("header debe ser un string CSV o una lista de strings")

        if self._body:
            if self._header != header_list:
                raise RuntimeError(
                    f"No se puede setear el header: el batch ya tiene filas y el header es distinto "
                    f"(existente: {self._header}, nuevo: {header_list})"
                )
            return

        self._header = header_list

    def is_last_batch(self):
        return self._last_batch

    def set_last_batch(self, status=True):
        self._last_batch = status

    def get_header(self):
        return self._header

    def get_query_id(self):
        return self._query_id

    def set_query_id(self, id):
        self._query_id = id

    def set_client_id(self, client_id):
        self._client_id = client_id

    def reset_body_and_increment_id(self):
        self._id_batch += 1
        self._body = []
        self._size = 0

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

    def is_empty(self):
        return len(self) == 0

    def __iter__(self):
        """Permite iterar sobre las filas del body"""
        return iter(self._body)

    def encode(self) -> bytes:
        res = b''
        res += self._id_batch.to_bytes(4, byteorder='big', signed=False)
        res += self._query_id.to_bytes(4, byteorder='big', signed=False)
        res += self._client_id.to_bytes(4, byteorder='big', signed=False)
        if self._last_batch:
            res += (1).to_bytes(1, byteorder="big", signed=False)
        else:
            res += (0).to_bytes(1, byteorder="big", signed=False)
        type_bytes = self._type_file.encode("utf-8")[:1]
        if not type_bytes:
            type_bytes = b"\x00"
        res += type_bytes
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
        self._query_id = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4
        self._client_id = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4
        self._last_batch = bool(int.from_bytes(data[offset:offset + 1], byteorder='big', signed=False))
        offset += 1
        self._type_file = data[offset:offset + 1].decode("utf-8", errors='replace')
        offset += 1
        header_len = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4
        header_bytes = data[offset:offset + header_len]
        self._header = header_bytes.decode("utf-8", errors='replace').split(",") if header_len > 0 else []
        offset += header_len

        self._size = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4

        body_len = int.from_bytes(data[offset:offset + 4], byteorder='big', signed=False)
        offset += 4

        body_bytes = data[offset:offset + body_len]
        body_str = body_bytes.decode("utf-8", errors='replace')
        self._body = [line.split(",") for line in body_str.split("|")] if body_len > 0 else []

    def __str__(self):
        lines = [
            f"Batch ID: {self._id_batch}",
            f"Query ID: {self._query_id}",
            f"Client ID: {self._client_id}",
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

    def add_row(self, row):
        """
        agrega una fila al batch.
        - row puede ser:
            * string con formato csv (ej: "1,Juan,20")
            * lista de strings (ej: ["1", "Juan", "20"])
        """
        if isinstance(row, str):
            row_list = row.strip().split(",")
        elif isinstance(row, list):
            row_list = row
        else:
            raise TypeError("row debe ser un string csv o una lista de strings")

        if self._header and len(row_list) != len(self._header):
            raise ValueError(
                f"La fila tiene {len(row_list)} columnas pero el header tiene {len(self._header)}"
            )

        self._body.append(row_list)
        self._size += 1

    def replace_all_rows(self, new_rows):
        """
        reemplaza todas las filas del batch con `new_rows`.
        Puede recibir lista de listas o lista de strings CSV.
        Valida que la cantidad de columnas coincida con headers.
        """
        if self._header is None:
            raise ValueError("Primero hay que definir headers con set_headers().")

        processed_rows = []

        for row in new_rows:
            if isinstance(row, str):
                row = [col.strip() for col in row.split(",")]

            if not isinstance(row, list):
                raise TypeError("Cada fila debe ser lista o string CSV.")

            if len(row) != len(self._header):
                raise ValueError(
                    f"Fila inválida: {row}, cantidad esperada: {len(self._header)}"
                )
            processed_rows.append(row)
        self._body = processed_rows
        self._size = len(processed_rows)

    def delete_rows(self):
        self._body = []
        self._size = 0
        
    def add_rows(self, rows):
        """
        agrega muchas filas al batch.
        - rows puede ser:
            * lista de strings CSV (ej: ["1,Juan,20", "2,Ana,30"])
            * lista de listas de strings (ej: [["1","Juan","20"], ["2","Ana","30"]])
        """
        if not isinstance(rows, list):
            raise TypeError("rows tiene q ser una lista de strings csv o una lista de listas")

        for row in rows:
            self.add_row(row)

#
# aux = Batch(5, 1, 10, False, 't', ['id', 'b', 'c'], [['1', 'rwe23', '23edwq'], ['2', 'efw', 'ewq'], ['3', '2w',
# '3r']])
#
# print(aux)
# bytes=aux.encode()
# aux2 = Batch()
# aux2.decode(bytes)
# print(aux2)

#
# dic = {'1': 'Juan', '2': 'Ana', '4': 'dada'}
#
# print(aux)
# try:
#     aux.change_header_name_value('id', 'name', dic)
# except Exception as e:
#     print(e)
# print(aux)
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
