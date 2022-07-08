#!/bin/bash
# Let's first generate some datasets
python3 benchmark.py generate-data --path="$(pwd)/dataset/50000000_1_parquet/" --nrows=50_000_000 --nrandom-cols=1

# Let's then benchmark the read
python3 benchmark.py bench-read-data --path="$(pwd)/dataset/50000000_1_parquet/"


