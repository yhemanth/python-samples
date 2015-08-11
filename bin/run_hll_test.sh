#!/bin/sh
for i in `seq 1 10`
do
  python data-utils/generate_distinct_ids.py 25000 1000 50 1000000000 data ids_100000_run$i;
  python sketches/sketches/redis_hll_intersects.py data/ids_100000_run${i}_1 data/ids_100000_run${i}_2;
done
