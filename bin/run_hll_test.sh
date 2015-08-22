#!/bin/sh
if [ $# -lt 5 ]
then
  echo "Usage: $0 num_runs set_a_cardinality set_b_cardinality intersection_cardinality data_dir [generate]";
  exit -1;
fi
num_runs=$1
card_a=$2
card_b=$3
card_a_n_b=$4
data_dir=$5
for i in `seq 1 $num_runs`
do
  file_name_prefix=ids_$2_$3_$4_run$i;
  if [ $# -eq 6 ]
  then
    python data-utils/generate_distinct_ids.py ${card_a} ${card_b} ${card_a_n_b} 1000000000 ${data_dir} ${file_name_prefix};
  fi
  python sketches/sketches/redis_hll_intersects.py ${data_dir}/${file_name_prefix}_1 ${data_dir}/${file_name_prefix}_2;
done
