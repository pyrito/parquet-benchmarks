#!/bin/bash
# Let's first generate some datasets 
skinny=(50_000_000 100_000_000 500_000_000 750_000_000 900_000_000)
for NROWS in "${skinny[@]}"
do
    python3 benchmark.py generate-data --single-file --path="$(pwd)/dataset/${NROWS}__1_data.parquet" --nrows=$NROWS --nrandom-cols=1
done

# Just wait for a bit before running the read benchmarks
sleep 30 

# Clear the caches before any IO intensive benchmarking
sudo sh -c 'echo 1 >/proc/sys/vm/drop_caches'
sudo sh -c 'echo 2 >/proc/sys/vm/drop_caches'
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'

# Let's then benchmark the read
for NROWS in "${skinny[@]}"
do  
    python3 benchmark.py bench-read-data --path="$(pwd)/dataset/${NROWS}__1_data.parquet"
done
