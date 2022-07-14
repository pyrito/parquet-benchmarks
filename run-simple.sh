#!/bin/bash
# Let's first generate some datasets 
NROWS=67000000
NCOLS=4
FACTOR=30
ROWGROUPSIZE=$((NROWS/FACTOR))
echo $ROWGROUPSIZE
python3 benchmark.py generate-data --single-file --path="$(pwd)/dataset/${NROWS}__${NCOLS}_data.parquet" --nrows=$NROWS --nrandom-cols=$NCOLS --row-group-size=$ROWGROUPSIZE

sleep 5

python3 benchmark.py bench-read-data --path="$(pwd)/dataset/${NROWS}__${NCOLS}_data.parquet" --warm-cache
