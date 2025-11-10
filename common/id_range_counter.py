SET_POSITION = 0
RANGES_POSITION = 1


class IDRangeCounter:
    def __init__(self):
        # self.individual_ids = set()
        # self.ranges = []
        self.dic_files = {}

    def already_processed(self, id: int, file_type: str):
        if file_type not in self.dic_files:
            return False

        if id < 0:
            raise ValueError("id menor a cero")

        if id in self.dic_files[file_type][SET_POSITION]:
            return True

        for start, end in self.dic_files[file_type][RANGES_POSITION]:
            if start <= id <= end:
                return True
        return False

    def add_id(self, new_id: int, file_type: str):
        if self.already_processed(new_id, file_type):
            return

        if file_type not in self.dic_files:
            self.dic_files[file_type] = [set(), []]
            self.dic_files[file_type][SET_POSITION].add(new_id)
            self.dic_files[file_type][RANGES_POSITION].append((new_id, new_id))
            return

        self.dic_files[file_type][SET_POSITION].add(new_id)

        merged = False
        new_ranges = []

        current_start, current_end = new_id, new_id

        for start, end in self.dic_files[file_type][RANGES_POSITION]:
            if end < current_start - 1:
                new_ranges.append((start, end))
            elif start > current_end + 1:
                new_ranges.append((current_start, current_end))
                current_start, current_end = start, end
            else:
                current_start = min(current_start, start)
                current_end = max(current_end, end)
                merged = True

        new_ranges.append((current_start, current_end))

        self.dic_files[file_type][RANGES_POSITION] = sorted(new_ranges)

        if merged:
            final_start = min(r[0] for r in new_ranges if r[0] <= new_id <= r[1])
            final_end = max(r[1] for r in new_ranges if r[0] <= new_id <= r[1])

            ids_to_remove = set()
            for id_val in self.dic_files[file_type][SET_POSITION]:
                if final_start <= id_val <= final_end:
                    ids_to_remove.add(id_val)
            self.dic_files[file_type][SET_POSITION] -= ids_to_remove

    def add_range(self, start: int, end: int, file_type: str):
        if start > end or start < 0:
            raise ValueError(f"Invalid range")

        if file_type not in self.dic_files:
            self.dic_files[file_type] = [set(), []]
            self.dic_files[file_type][RANGES_POSITION].append((start, end))
            return

        new_ranges = []
        current_start, current_end = start, end
        for r_start, r_end in self.dic_files[file_type][RANGES_POSITION]:
            if r_end < current_start - 1:
                new_ranges.append((r_start, r_end))
            elif r_start > current_end + 1:
                new_ranges.append((current_start, current_end))
                current_start, current_end = r_start, r_end
            else:
                current_start = min(current_start, r_start)
                current_end = max(current_end, r_end)

        new_ranges.append((current_start, current_end))
        self.dic_files[file_type][RANGES_POSITION] = sorted(new_ranges)
        ids_to_remove = set()
        for id_val in self.dic_files[file_type][SET_POSITION]:
            final_range = next(
                ((s, e) for s, e in self.dic_files[file_type][RANGES_POSITION] if s <= start <= e or s <= end <= e),
                None)
            if final_range:
                final_start, final_end = final_range
                if final_start <= id_val <= final_end:
                    ids_to_remove.add(id_val)
        self.dic_files[file_type][SET_POSITION] -= ids_to_remove

    def __str__(self):
        return str(self.dic_files)

    # def len(self, type: str):
    #     total_count = 0
    #     ranges = self.dic_files.get(type)[RANGES_POSITION]
    #     ids = self.dic_files.get(type)[SET_POSITION]

    #     for start, end in ranges:
    #         total_count += (end - start + 1)
    #     total_count += len(ids)
            
    #     return total_count

# counter = IDRangeCounter()

# counter.add_id(1, 'a')
# print(counter.already_processed(1, 'a'))
# counter.add_id(3, 'a')
# print(counter.already_processed(3, 'a'))
# counter.add_id(5, 'a')
# print(counter.already_processed(5, 'a'))
# counter.add_id(7, 'a')
# print(counter.already_processed(7, 'a'))

# counter.add_id(1, 'b')
# print(counter.already_processed(1, 'b'))
# counter.add_id(3, 'b')
# print(counter.already_processed(3, 'b'))
# counter.add_id(5, 'b')
# print(counter.already_processed(5, 'b'))
# counter.add_id(7, 'b')
# print(counter.already_processed(7, 'b'))
# print(counter)
# #
# counter.add_id(2, 'a')
# print(counter.already_processed(2, 'a'))
# print(counter)
# #
# counter.add_id(6, 'a')
# print(counter.already_processed(6, 'a'))
# counter.add_id(10, 'a')
# counter.add_id(20, 'a')
# print(counter.already_processed(20, 'a'))
# counter.add_id(0, 'a')
# print(counter)
# print("len: ", counter.len('a'))
#
# counter.add_id(4, 'a')
# print(counter)
#
# counter.add_range(8, 21, 'a')
# print(counter)
#
# counter.add_range(8, 21, 'c')
# print(counter)