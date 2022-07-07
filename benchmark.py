import time

import numpy as np
import pandas as pd

import modin.pandas as mpd
import modin.config as cfg
import ray

cfg.BenchmarkMode.put(True)

def generate_data(path, nrows=5000, n_random_cols=100):
    data = {f"col{i}":  np.random.rand(nrows) for i in range(n_random_cols)}

    df = mpd.DataFrame(data)
    df.to_parquet(path)


if __name__ == "__main__":

    path = "/Users/kvelayutham/Documents/parquet_benchmarks/dataset"
    generate_data(path, nrows=1_000_000_000, n_random_cols=1)

    t = time.time()
    pdf = pd.read_parquet(path)
    print(f"pandas read_parquet time: {time.time() - t} s")

    t = time.time()
    mdf = mpd.read_parquet(path)
    print(f"modin read_parquet time: {time.time() - t} s")

    print(f"Original shape: {pdf.shape}")