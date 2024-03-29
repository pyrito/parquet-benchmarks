import time

import click
import numpy as np
from pathlib import Path
import pandas
import os

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
def generate_data(path, nrows, nrandom_cols, single_file, row_group_size):
    if not single_file:
        ensure_dir(path)
    data = np.random.randint(0, 100, size=(nrows, nrandom_cols))
    df = pandas.DataFrame(data).add_prefix('col')
    df.to_parquet(str(path), row_group_size=row_group_size)
    print(f"Parquet files written to {path}")


@click.command(help="Benchmark reading parquet datasets")
@click.option("--path", type=Path, default=default_path, help="Path for where to read datasets")
@click.option("--clear-cache", is_flag=True, help="If we should clear the cache between every read")
@click.option("--warm-cache", is_flag=True, help="If we want to keep the cache warm for the dataset read")
def bench_read_data(path, clear_cache, warm_cache):
    if warm_cache:
        warm_cache_df(path)
 
    if clear_cache:
        clear_cache_os()

    t = time.time()
    pdf = pandas.read_parquet(path)
    pd_read_parquet_time = time.time() - t
    
    if clear_cache:
        clear_cache_os()
    
    print(f"pandas read_parquet time: {pd_read_parquet_time} s")
    print(f"Original shape: {pdf.shape}\n")
    
run.add_command(generate_data)
run.add_command(bench_read_data)


if __name__ == '__main__':
    run()
