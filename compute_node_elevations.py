import gc
import os

from dask.distributed import Client
import dask.dataframe as dd

from relevation import get_elevation

data_dir="/home/ross/repos/refinement/data"
gc.set_threshold(10000, 10, 10)

def _get_elevation_for_row(row):
    return get_elevation(row.lat, row.lon)

def main():
    client = Client()
    nodes_df = dd.read_parquet(  # type: ignore
        os.path.join(data_dir, "nodes")
    )
    elevations = nodes_df.apply(  # type: ignore
        _get_elevation_for_row,
        axis=1,
        meta=("elevation", "float"),
    )
    nodes_df = nodes_df.assign(elevation=elevations)  # type: ignore
    nodes_df.to_parquet(
        os.path.join(data_dir, "node_elevations"),
        partition_on=["bucket"],
    )
    client.close()

if __name__ == '__main__':
    main()