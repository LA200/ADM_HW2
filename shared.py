import os
import argparse
import numpy as np
import pandas as pd
import math
import itertools
from matplotlib import pyplot as plt
import seaborn as sns

# >> In case we prefer dash to pandas we should install it using the following commands:
# !python3 -m pip install "dask[complete]" --upgrade
# !python3 -m pip install "dask[array]" --upgrade
# !python3 -m pip install "dask[bag]" --upgrade
# !python3 -m pip install "dask[dataframe]" --upgrade
# !python3 -m pip install "dask[delayed]" --upgrade
# !python3 -m pip install "dask[distributed]" --upgrade
# import dask.dataframe as dd
# >> Now we could call similar functions we would use in pandas but in memory efficient way

# Datasets to be analyzed
df_labels = ['2019-Oct', '2019-Nov']

# Set up defaults that are to be used by functions corresponding to different RQs
default_nrows = None
deafult_size_mb = 1_000  # divider for chunk size
default_file_label = df_labels[0]
default_aws = False

dataset_info = {
    '2019-Oct': {
        'total_n_rows': 42_448_764,
        'total_size_mb': 5407
    },
    '2019-Nov': {
        'total_n_rows': 67_501_979,
        'total_size_mb': 8590
    },
}


def get_chunksize(df_label: str = default_file_label, size_mb: float = deafult_size_mb):
    """
    Function to calculate chunk size based on memory we're ready to allocate while processing the file
    """
    if size_mb is not None:
        total_size_mb = dataset_info.get(df_label, dict()).get('total_size_mb', 7000)
        n_chunks = max(1, np.ceil(total_size_mb / size_mb))
        total_n_rows = dataset_info.get(df_label, dict()).get('total_n_rows', None)
        if total_n_rows:
            return int(np.ceil(total_n_rows / n_chunks))
    return None


def get_file_path(df_label=default_file_label, aws=False):
    """
    Function to retrieve by a label either *.csv path of a file stored locally
    or request downloading from a file located on AWS S3
    """
    if aws:
        return f"s3://sapienza2020adm/ecommerce/{df_label}.csv"
    return f"datasets/{df_label}.csv"


def read_csv(
    df_label: str = default_file_label,
    aws: bool = default_aws,
    size_mb: float = deafult_size_mb,
    nrows: int = default_nrows,
    usecols: list = None,
    dtype: dict = None,
    parse_dates: list = False,
    date_parser=None
):
    """
    Key function to retrieve data (in chunks by default)
    """
    return pd.read_csv(
        get_file_path(df_label=df_label, aws=aws),
        usecols=usecols,
        dtype=dtype,
        engine='c',
        na_filter=False,
        memory_map=True,
        chunksize=get_chunksize(df_label=df_label, size_mb=size_mb),
        nrows=nrows,
        parse_dates=parse_dates,
        date_parser=date_parser
    )
