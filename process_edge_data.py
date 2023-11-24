from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import logging
import warnings

from relevation import get_elevation, get_distance_and_elevation_change


logger = logging.getLogger("distributed.utils_perf")
logger.setLevel(logging.ERROR)
warnings.filterwarnings(action="ignore", category=FutureWarning)


def get_elevation_for_row(row):
    return get_elevation(row.lat, row.lon)


def get_edge_distance_and_elevation_for_row(row):
    return get_distance_and_elevation_change(
        row.start_lat, row.start_lon, row.end_lat, row.end_lon
    )


if __name__ == "__main__":
    client = Client()

    edges_df = dd.read_parquet("./data/edges/")
    edge_elevations = edges_df.apply(
        get_edge_distance_and_elevation_for_row,
        axis=1,
        meta={
            0: "float",
            1: "float",
            2: "float",
        },
        result_type="expand",
    )
    edge_elevations.columns = [
        "dist_change",
        "elevation_gain",
        "elevation_loss",
    ]
    # edge_elevations = edge_elevations.merge(
    #     right=edges_df.loc[:, ["bucket"]],
    #     left_index=True,
    #     right_index=True,
    # )
    edge_elevations = edge_elevations.assign(bucket=edges_df.bucket)
    edge_elevations.to_parquet(
        "./data/edge_elevations/", partition_on=["bucket"]
    )
