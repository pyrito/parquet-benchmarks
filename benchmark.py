import time

import click
import numpy as np
from pathlib import Path
import pandas as pd

import modin.pandas as mpd
import modin.config as cfg
import ray

cfg.BenchmarkMode.put(True)

default_path = Path("default/")


@click.group()
def run():
    pass


@click.command(help="Generate parquet datasets")
@click.option("--path", type=Path, default=default_path, help="Path for where to write datasets")
@click.option("--nrows", type=int, default=5000, help="Number of rows of DataFrame -> parquet (default 5000)")
@click.option("--nrandom-cols", type=int, default=100, help="Number of columns of DataFrame -> parquet (default 100)")
def generate_data(path, nrows=5000, n_random_cols=100):
    data = {f"col{i}":  np.random.rand(nrows) for i in range(n_random_cols)}

    df = mpd.DataFrame(data)
    df.to_parquet(path)


@click.command(help="Benchmark reading parquet datasets")
@click.option("--path", type=Path, default=default_path, help="Path for where to read datasets")
def bench_read_data(path):
    t = time.time()
    pdf = pd.read_parquet(path)
    print(f"pandas read_parquet time: {time.time() - t} s")

    t = time.time()
    mdf = mpd.read_parquet(path)
    print(f"modin read_parquet time: {time.time() - t} s")

    print(f"Original shape: {pdf.shape}")


run.add_command(generate_data)
run.add_command(bench_read_data)


if __name__ == '__main__':
    run()
