import time

import click
import numpy as np
from pathlib import Path
import pandas
import os

import modin.pandas as pd
import modin.config as cfg
import ray

cfg.BenchmarkMode.put(True)

# Create an empty Modin dataframe and throw it away
# We do this so that we allow Modin to give specific hints for Ray
t = pd.DataFrame()

default_path = Path("dataset/")

def ensure_dir(path):
    if not os.path.exists(path):    
        os.makedirs(path)

 
@click.group()
def run():
    pass


@click.command(help="Generate parquet datasets")
@click.option("--path", type=Path, default=default_path, help="Path for where to write datasets")
@click.option("--nrows", type=int, default=5000, help="Number of rows of DataFrame -> parquet (default 5000)")
@click.option("--nrandom-cols", type=int, default=100, help="Number of columns of DataFrame -> parquet (default 100)")
@click.option("--single-file", is_flag=True, help="If script should output a single parquet file or not")
def generate_data(path, nrows, nrandom_cols, single_file):
    if not single_file:
        ensure_dir(path)
    data = {f"col{i}":  np.random.rand(nrows) for i in range(nrandom_cols)}
    
    df = pandas.DataFrame(data) if single_file else pd.DataFrame(data)
    df.to_parquet(str(path))
    print(f"Parquet files written to {path}")


@click.command(help="Benchmark reading parquet datasets")
@click.option("--path", type=Path, default=default_path, help="Path for where to read datasets")
def bench_read_data(path):
    ensure_dir(path)
    t = time.time()
    pdf = pandas.read_parquet(path)
    pd_read_parquet_time = time.time() - t

    t = time.time()
    mdf = pd.read_parquet(path)
    mpd_read_parquet_time = time.time() - t
    
    print(f"pandas read_parquet time: {pd_read_parquet_time} s")
    print(f"modin read_parquet time: {mpd_read_parquet_time} s")
    print(f"Original shape: {pdf.shape}\n")


run.add_command(generate_data)
run.add_command(bench_read_data)


if __name__ == '__main__':
    run()
