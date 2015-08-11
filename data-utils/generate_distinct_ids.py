import random
import sys


class IDGenerator:
    def __init__(self, cardinality, range_max):
        self.range_max = range_max
        self.cardinality = cardinality

    def generate(self, file):
        ids = set()
        count = 0
        while count < self.cardinality:
            next_num = random.randint(1, self.range_max)
            if next_num not in ids:
                ids.add(next_num)
                count += 1
                file.write(str(next_num)+'\n')

if __name__ == "__main__":
    idGen = IDGenerator(int(sys.argv[1]), int(sys.argv[2]))
    ids_file = open(sys.argv[3], 'w')
    idGen.generate(ids_file)
