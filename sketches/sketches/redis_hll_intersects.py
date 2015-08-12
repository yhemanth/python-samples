import os
import sys

import redis

REDIS_PIPELINE_BATCH_SIZE = 10000


class RedisHllIntersects:
    def __init__(self, host='127.0.0.1', port=6379):
        self.redis_client = redis.StrictRedis(host, port)

    def intersect_ids(self, ids_files):
        common_elements, k1, k2, s1_count_actual, s2_count_actual = self._add_keys(ids_files)
        self._intersection_counts(common_elements, k1, k2, s1_count_actual, s2_count_actual)
        self._cleanup(k1, k2)

    def _cleanup(self, k1, k2):
        self.redis_client.delete(k1, k2, self._common_key_name(k1, k2))

    def _intersection_counts(self, common_elements, k1, k2, s1_count_actual, s2_count_actual):
        k1_k2 = self._common_key_name(k1, k2)
        self.redis_client.pfmerge(k1_k2, k1, k2)
        k1_count = self.redis_client.pfcount(k1)
        k2_count = self.redis_client.pfcount(k2)
        k1_k2_count = self.redis_client.pfcount(k1_k2)
        counts_str = ",".join(map(lambda x: str(x),
                                  [s1_count_actual, k1_count, s2_count_actual, k2_count, len(common_elements),
                                   (k1_count + k2_count - k1_k2_count)]))
        print ",".join([self._common_key_name(k1, k2),counts_str])

    @staticmethod
    def _common_key_name(k1, k2):
        return "temp" + ":".join([k1, k2])

    def _add_keys(self, ids_files):
        k1 = os.path.basename(ids_files[0])
        common_elements = self._add_ids(ids_files[0], k1)
        s1_count_actual = len(common_elements)
        k2 = os.path.basename(ids_files[1])
        second_set = self._add_ids(ids_files[1], k2)
        common_elements = second_set.intersection(common_elements)
        return common_elements, k1, k2, s1_count_actual, len(second_set)

    def _add_ids(self, ids_file, hll_key_name):
        self.redis_client.delete(hll_key_name)
        s = set()
        map(lambda x: s.add(x), open(ids_file, 'r').readlines())
        self._add_ids_to_hll(hll_key_name, s)
        return s

    def _add_ids_to_hll(self, key, hll_set):
        pipeline = self.redis_client.pipeline()
        batch_size = REDIS_PIPELINE_BATCH_SIZE
        for hll_id in hll_set:
            pipeline.pfadd(key, hll_id)
            batch_size -= 1
            if batch_size == 0:
                pipeline.execute()
                batch_size = REDIS_PIPELINE_BATCH_SIZE
        pipeline.execute()


if __name__ == "__main__":
    redisHllTester = RedisHllIntersects()
    redisHllTester.intersect_ids([sys.argv[1], sys.argv[2]])
