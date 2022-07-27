import time

import click
import numpy as np
from pathlib import Path
import pandas
import os

import modin.pandas as pd
import modin.config as cfg
import ray

# Enable modin logs
cfg.LogMode.enable()

# Enable benchmark mode
cfg.BenchmarkMode.put(True)

# Initialize ray
ray.init()

default_path = Path("dataset/")

def ensure_dir(path):
    if not os.path.exists(path):    
        os.makedirs(path)


def clear_cache_os():
    os.system("sudo sh -c \'echo 1 >/proc/sys/vm/drop_caches\'")
    os.system("sudo sh -c \'echo 2 >/proc/sys/vm/drop_caches\'")
    os.system("sudo sh -c \'echo 3 >/proc/sys/vm/drop_caches\'")


def warm_cache_df(path):
    warm_pdf = pandas.read_parquet(path)


@click.group()
def run():
    pass


@click.command(help="Generate parquet datasets")
@click.option("--path", type=Path, default=default_path, help="Path for where to write datasets")
@click.option("--nrows", type=int, default=5000, help="Number of rows of DataFrame -> parquet (default 5000)")
@click.option("--nrandom-cols", type=int, default=100, help="Number of columns of DataFrame -> parquet (default 100)")
@click.option("--single-file", is_flag=True, help="If script should output a single parquet file or not")
@click.option("--row-group-size", type=int, default=None, help="Row group size for parquet files")
@click.option("--set-index", is_flag=True, help="Set index to a random Int64Index")
def generate_data(path, nrows, nrandom_cols, single_file, row_group_size, set_index):
    if not single_file:
        ensure_dir(path)
    # data = {f"col{i}":  np.random.rand(nrows) for i in range(nrandom_cols)}
    data = np.random.randint(0, 100, size=(nrows, nrandom_cols))
    df = pandas.DataFrame(data).add_prefix('col') if single_file else pd.DataFrame(data).add_prefix('col')
    if set_index:
        print("Setting index...")
        df = df.set_index(pd.Index(np.random.randint(0, 100, size=nrows)))
    df.to_parquet(str(path), row_group_size=row_group_size)
    print(f"Parquet files written to {path}")


@click.command(help="Benchmark reading parquet datasets")
@click.option("--path", type=Path, default=default_path, help="Path for where to read datasets")
@click.option("--clear-cache", is_flag=True, help="If we should clear the cache between every read")
@click.option("--warm-cache", is_flag=True, help="If we want to keep the cache warm for the dataset read")
def bench_read_data(path, clear_cache, warm_cache):
    mdf = pd.DataFrame(np.zeros((10000,10000)))
    mdf.applymap(lambda x: x+ 1)
    # print(mdf._query_compiler._modin_frame._partitions.shape)
    # print(cfg.NPartitions.get())

    if warm_cache:
        warm_cache_df(path)
 
    if clear_cache:
        clear_cache_os()

    t = time.time()
    # pdf = pandas.read_parquet(path, columns=['col0']) 
    pd_read_parquet_time = time.time() - t
    
    # Time dtype retrieval for sanity 
    dtype_t = time.time()
    # print(pdf.dtypes)
    dtype_time = time.time() - dtype_t
    
    print(f"pandas read_parquet time: {pd_read_parquet_time} s")
    print(f"pandas dtype time: {dtype_time} s")
    
    if clear_cache:
        clear_cache_os()
    
    # Read it once before to avoid the Ray bug with the deserialize cost? 
    mdf = pd.read_parquet("/home/ubuntu/parquet-benchmarks/dataset/25000000__1_data.parquet", columns=['col0'])
    if clear_cache:
        clear_cache_os()
    
    t = time.time()
    mdf = pd.read_parquet(path, columns=['col0'])
    mpd_read_parquet_time = time.time() - t
    
    # Time dtype retrieval for sanity check
    mdtype_t = time.time()
    print(mdf.dtypes)
    mdtype_time = time.time() - mdtype_t

    print(f"modin read_parquet time: {mpd_read_parquet_time} s")
    print(f"modin dtype time: {mdtype_time} s")
    print(f"Original shape: {mdf.shape}\n")
    
    time.sleep(2)
    ray.timeline(f"read_parquet_{mdf.shape[0]}_{mdf.shape[1]}.json")

run.add_command(generate_data)
run.add_command(bench_read_data)


if __name__ == '__main__':
    run()
