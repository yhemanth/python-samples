import os
import random
import sys


class IDGenerator:
    def __init__(self, cardinalities, range_max):
        self.cardinalities = cardinalities
        self.range_max = range_max

    def generate(self, results_dir, file_prefix):
        ids_1 = set()
        ids_2 = set()

        self._generate_common_elements(ids_1, ids_2)
        self._generate_distinct_elements(self.cardinalities[0]-self.cardinalities[2], ids_1, ids_2)
        self._generate_distinct_elements(self.cardinalities[1]-self.cardinalities[2], ids_2, ids_1)

        self._write_elements(os.path.join(results_dir, file_prefix+"_1"), ids_1)
        self._write_elements(os.path.join(results_dir, file_prefix+"_2"), ids_2)

    def _generate_distinct_elements(self, required_count, set_to_add, other_set):
        distinct_count = 0
        while distinct_count < required_count:
            next_num = random.randint(1, self.range_max)
            if next_num not in set_to_add and next_num not in other_set:
                set_to_add.add(next_num)
                distinct_count += 1

    def _generate_common_elements(self, ids_1, ids_2):
        common_ids_count = 0
        while common_ids_count < self.cardinalities[2]:
            next_num = random.randint(1, self.range_max)
            if next_num not in ids_1:
                ids_1.add(next_num)
                ids_2.add(next_num)
                common_ids_count += 1

    @staticmethod
    def _write_elements(file_name, ids_set):
        f = open(file_name, 'w')
        for num in ids_set:
            f.write(str(num)+"\n")
        f.close()


if __name__ == "__main__":
    idGen = IDGenerator([int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])], int(sys.argv[4]))
    idGen.generate(sys.argv[5], sys.argv[6])
