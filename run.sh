#!/bin/bash
# Let's first generate some datasets 
skinny=(50000000 500000000 750000000 900000000)
# skinny=(50000000 75000000 100000000)
FACTOR=30
#for NROWS in "${skinny[@]}"
#do
#    ROWGROUPSIZE=$((NROWS/FACTOR))
#    echo $ROWGROUPSIZE
#    python3 benchmark.py generate-data --single-file --set-index --path="$(pwd)/dataset/${NROWS}__1_data.parquet" --nrows=$NROWS --nrandom-cols=1 --row-group-size=$ROWGROUPSIZE
##    #python3 benchmark.py generate-data --single-file --path="$(pwd)/dataset/1__${NROWS}_data.parquet" --nrows=1 --nrandom-cols=$NROWS
#done

# Just wait for a bit before running the read benchmarks
sleep 5 

# Let's then benchmark the read
for NROWS in "${skinny[@]}"
do  
    python3 benchmark.py bench-read-data --path="$(pwd)/dataset/${NROWS}__1_data.parquet" --clear-cache
    #python3 benchmark.py bench-read-data --path="$(pwd)/dataset/1__${NROWS}_data.parquet/" --clear-cache
done
