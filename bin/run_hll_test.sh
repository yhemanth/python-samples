#!/bin/sh

# Validate
if [ $# -lt 6 ]
then
  echo "Usage: $0 num_runs set_a_cardinality set_b_cardinality intersection_cardinality data_dir minhash_k [generate|profile]*";
  exit -1;
fi

# process required arguments
num_runs=$1
card_a=$2
card_b=$3
card_a_n_b=$4
data_dir=$5
minhash_k=$6
shift 6

# process optional arguments
generate_data=0
profile=0
while [[ $# -ge 1 ]]
do
  key=$1
  case $key in
    generate)
    generate_data=1
    ;;
    profile)
    profile=1
    ;;
    *)
    ;;
  esac
  shift
done

# run python code
for i in `seq 1 $num_runs`
do
  file_name_prefix=ids_${card_a}_${card_b}_${card_a_n_b}_run$i;
  if [ $generate_data -eq 1 ]
  then
    python data-utils/generate_distinct_ids.py ${card_a} ${card_b} ${card_a_n_b} 1000000000 ${data_dir} ${file_name_prefix};
  fi
  profile_string=""
  if [ $profile -eq 1 ]
  then
    profile_string="-m cProfile -o ${file_name_prefix}.prof"
  fi
  python ${profile_string} sketches/sketches/redis_hll_intersects.py ${data_dir}/${file_name_prefix}_1 ${data_dir}/${file_name_prefix}_2 ${minhash_k};
done
