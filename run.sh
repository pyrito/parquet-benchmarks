#!/bin/bash
# Let's first generate some datasets 
# skinny=(50_000_000 100_000_000 500_000_000 750_000_000 900_000_000)
# skinny=(1_000_000_000 5_000_000_000)
for NROWS in "${skinny[@]}"
do
    python3 benchmark.py generate-data --single-file --path="$(pwd)/dataset/${NROWS}__1_parquet/" --nrows=$NROWS --nrandom-cols=1
    #python3 benchmark.py generate-data --single-file --path="$(pwd)/dataset/1__${NROWS}_data.parquet" --nrows=1 --nrandom-cols=$NROWS
done

# Just wait for a bit before running the read benchmarks
#sleep 30 

# Let's then benchmark the read
for NROWS in "${skinny[@]}"
do  
    python3 benchmark.py bench-read-data --path="$(pwd)/dataset/${NROWS}__1_parquet/" --warm-cache
    #python3 benchmark.py bench-read-data --path="$(pwd)/dataset/1__${NROWS}_data.parquet/" --warm-cache
done
