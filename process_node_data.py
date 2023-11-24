from dask.distributed import Client
import dask.dataframe as dd
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
    nodes_df = dd.read_parquet("./data/nodes/")
    elevations = nodes_df.apply(
        get_elevation_for_row, axis=1, meta=("elevation", "float")
    )
    nodes_df = nodes_df.assign(elevation=elevations)
    nodes_df.to_parquet("./data/node_elevations/", partition_on=["bucket"])
