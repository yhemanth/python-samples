import os
import sys

import redis

REDIS_PIPELINE_BATCH_SIZE = 10000


def common_key_name(k1, k2):
    return "temp" + ":".join([k1, k2])


class IdSet:
    def __init__(self, ids_file, redis_client):
        self.redis_client = redis_client
        self.hll_key_name = os.path.basename(ids_file)
        self.ids = set()
        map(lambda x: self.ids.add(x), open(ids_file, 'r').readlines())

    def add_to_hll(self):
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

    def cleanup(self):
        self.redis_client.delete(self.hll_key_name)


class RedisHllIntersects:
    def __init__(self, host='127.0.0.1', port=6379):
        self.redis_client = redis.StrictRedis(host, port)

    def intersect_ids(self, ids_file1, ids_file2):
        id_set1 = IdSet(ids_file1, self.redis_client)
        id_set1.add_to_hll()

        id_set2 = IdSet(ids_file2, self.redis_client)
        id_set2.add_to_hll()

        merged_key_name, hll_intersection_count = id_set1.intersect_hlls_using_inclusion_exclusion(id_set2)

        counts_str = ",".join(map(lambda x: str(x),
                                  [id_set1.actual_count(), id_set1.hll_count(),
                                   id_set2.actual_count(), id_set2.hll_count(),
                                   id_set1.intersection_count(id_set2),
                                   hll_intersection_count]))
        print ",".join([merged_key_name, counts_str])

        id_set1.cleanup()
        id_set2.cleanup()
        self.redis_client.delete(merged_key_name)


if __name__ == "__main__":
    redisHllTester = RedisHllIntersects()
    redisHllTester.intersect_ids(sys.argv[1], sys.argv[2])
