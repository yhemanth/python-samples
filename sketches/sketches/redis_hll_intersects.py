import os
import sys

import redis
from kminhash import KMinHash

REDIS_PIPELINE_BATCH_SIZE = 10000


def common_key_name(k1, k2):
    return "temp" + ":".join([k1, k2])


class IdSet:
    def __init__(self, ids_file, redis_client, minhash_k):
        self.redis_client = redis_client
        self.hll_key_name = os.path.basename(ids_file)
        self.ids = set()
        map(lambda x: self.ids.add(x), open(ids_file, 'r').readlines())
        self.minhash_set = KMinHash(minhash_k, self.redis_client, "mh" + self.hll_key_name)

    def add_to_sketches(self, minhash_strategy="mem_optimized"):
        self.__add_to_hll()
        if minhash_strategy == "pipelined":
            self.__add_to_kminhash_pipelined()
        else:
            self.__add_to_kminhash_mem_optimized()

    def __add_to_kminhash_mem_optimized(self):
        for hll_id in self.ids:
            self.minhash_set.update_min_hash(hll_id)

    def __add_to_kminhash_pipelined(self):
        self.minhash_set.initialize()
        batch_size = REDIS_PIPELINE_BATCH_SIZE
        ids_batch = list()
        for hll_id in self.ids:
            ids_batch.append(hll_id)
            batch_size -= 1
            if batch_size == 0:
                self.minhash_set.update_min_hashes_batch(ids_batch)
                batch_size = REDIS_PIPELINE_BATCH_SIZE
                ids_batch = list()
        self.minhash_set.update_min_hashes_batch(ids_batch)

    def __add_to_hll(self):
        self.redis_client.delete(self.hll_key_name)
        pipeline = self.redis_client.pipeline()
        batch_size = REDIS_PIPELINE_BATCH_SIZE
        for hll_id in self.ids:
            pipeline.pfadd(self.hll_key_name, hll_id)
            batch_size -= 1
            if batch_size == 0:
                pipeline.execute()
                batch_size = REDIS_PIPELINE_BATCH_SIZE
        pipeline.execute()

    def actual_count(self):
        return len(self.ids)

    def hll_count(self):
        return self.redis_client.pfcount(self.hll_key_name)

    def intersection_count(self, other_id_set):
        return len(self.ids.intersection(other_id_set.ids))

    def intersect_hlls_using_inclusion_exclusion(self, other_id_set):
        merged_key_name = common_key_name(self.hll_key_name, other_id_set.hll_key_name)
        self.redis_client.pfmerge(merged_key_name, self.hll_key_name, other_id_set.hll_key_name)
        intersection_count = self.hll_count() + other_id_set.hll_count() - self.redis_client.pfcount(merged_key_name)
        return merged_key_name, intersection_count

    def intersect_using_kminhash(self, other_id_set):
        merged_key_name = common_key_name(self.hll_key_name, other_id_set.hll_key_name)
        self.redis_client.pfmerge(merged_key_name, self.hll_key_name, other_id_set.hll_key_name)
        union_count = self.redis_client.pfcount(merged_key_name)
        jaccard_coefficient = self.minhash_set.estimate_jaccard_coefficient(other_id_set.minhash_set)
        return int(jaccard_coefficient * union_count)

    def cleanup(self):
        self.redis_client.delete(self.hll_key_name)


class RedisHllIntersects:
    def __init__(self, host='127.0.0.1', port=6379):
        self.redis_client = redis.StrictRedis(host, port)

    def intersect_ids(self, ids_file1, ids_file2, minhash_k, minhash_strategy):
        id_set1 = IdSet(ids_file1, self.redis_client, minhash_k)
        id_set1.add_to_sketches(minhash_strategy)

        id_set2 = IdSet(ids_file2, self.redis_client, minhash_k)
        id_set2.add_to_sketches(minhash_strategy)

        merged_key_name, hll_intersection_count = id_set1.intersect_hlls_using_inclusion_exclusion(id_set2)
        min_hash_intersection_count = id_set1.intersect_using_kminhash(id_set2)

        counts_str = ",".join(map(lambda x: str(x),
                                  [id_set1.actual_count(), id_set1.hll_count(),
                                   id_set2.actual_count(), id_set2.hll_count(),
                                   id_set1.intersection_count(id_set2),
                                   hll_intersection_count, min_hash_intersection_count]))
        print ",".join([merged_key_name, counts_str])

        id_set1.cleanup()
        id_set2.cleanup()
        self.redis_client.delete(merged_key_name)


if __name__ == "__main__":
    redisHllTester = RedisHllIntersects()
    redisHllTester.intersect_ids(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4])
