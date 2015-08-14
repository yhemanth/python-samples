import random
import sys


class KMinSet:
    def __init__(self, k, range_max=sys.maxint):
        self.range_max = range_max
        self.k = k
        self.kmin_uniques = list()
        self.max_kval = sys.maxint

    def update_min_set(self, val):
        normalized_val = float(val)/self.range_max
        if normalized_val < self.max_kval:
            self.kmin_uniques.append(normalized_val)
            self.kmin_uniques = list(set(self.kmin_uniques))
            if len(self.kmin_uniques) > self.k:
                self.kmin_uniques = sorted(self.kmin_uniques)[:self.k]
        self.max_kval = max(self.kmin_uniques)

    def cardinality(self):
        return int((self.k - 1)/self.max_kval)


def run_kmin_test():
    kminset = KMinSet(k, input_range)
    expected_set = set()
    for i in range(1, input_range):
        next_num = random.randint(1, input_range)
        expected_set.add(next_num)
        kminset.update_min_set(next_num)
    estimated_cardinality = kminset.cardinality()
    expected_cardinality = len(expected_set)
    print "Estimated cardinality: ", estimated_cardinality
    print "Expected cardinality: ", expected_cardinality
    print "Error percent: ", (float(abs(estimated_cardinality - expected_cardinality)) * 100) / expected_cardinality


if __name__ == "__main__":
    input_range = 1000000
    k = 128
    runs = 100
    for i in range(0, runs):
        print "Running test ", i
        run_kmin_test()
