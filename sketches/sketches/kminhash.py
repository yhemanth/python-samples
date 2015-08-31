import mmh3 as mmh3
import redis


class KMinHash:
    def __init__(self, k, redis_client, key):
        self.key = key
        self.redis_client = redis_client
        self.k = k
        self.redis_client.delete(self.key)

    def update_min_hash(self, element_id):
        min_hash = mmh3.hash(str(element_id))
        if self.redis_client.zcard(self.key) == self.k:
            max_score = self.redis_client.zrange(self.key, -1, -1, withscores=True)[0][1]
            # Is new element going to change k min hashes?
            if min_hash >= max_score:
                return
            else:
                # Remove the element with max score
                self.redis_client.zremrangebyrank(self.key, -1, -1)
        self.redis_client.zadd(self.key, min_hash, element_id)

    def estimate_jaccard_coefficient(self, other_min_hash):
        union_min_hash_name = ":".join(["minhash_union", self.key, other_min_hash.key])
        self.redis_client.delete(union_min_hash_name)
        self.redis_client.zunionstore(union_min_hash_name, [self.key, other_min_hash.key], aggregate='MIN')
        self.redis_client.zremrangebyrank(union_min_hash_name, self.k, -1)

        inter_min_hash_name = ":".join(["minhash_inter", union_min_hash_name, self.key, other_min_hash.key])
        self.redis_client.delete(inter_min_hash_name)
        self.redis_client.zinterstore(inter_min_hash_name, [union_min_hash_name, self.key, other_min_hash.key],
                                      aggregate='MIN')
        return float(self.redis_client.zcard(inter_min_hash_name)) / self.k

    def __str__(self):
        return self.key + ": " + str(self.redis_client.zrange(self.key, 0, -1, withscores=True))

if __name__ == "__main__":
    set_a = [3, 1, 4, 1, 5, 7, 9, 2]
    mh_set_a = KMinHash(5, redis.StrictRedis('127.0.0.1', 6379), 'a')
    for elem in set_a:
        mh_set_a.update_min_hash(elem)
    print "Set A: ", str(mh_set_a)

    set_b = [2, 1, 7, 9, 8, 1, 2, 4]
    mh_set_b = KMinHash(5, redis.StrictRedis('127.0.0.1', 6379), 'b')
    for elem in set_b:
        mh_set_b.update_min_hash(elem)
    print "Set B: ", str(mh_set_b)

    jaccard_coefficient = mh_set_a.estimate_jaccard_coefficient(mh_set_b)
    print "Estimated Jaccard coefficient: ", jaccard_coefficient

    s_a = set(set_a)
    s_b = set(set_b)
    actual_jaccard_coefficient = float(len(s_a.intersection(s_b))) / len(s_a.union(s_b))
    print "Actual Jaccard coefficient: ", actual_jaccard_coefficient

