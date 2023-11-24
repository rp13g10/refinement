"""Primary processing script, triggers the conversion of raw OSM data into
a networkx graph"""

import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from refinement.containers import RouteConfig
from refinement.enricher import GraphEnricher
from refinement.enricher.tagger import (
    GraphTagger,
)

# from refinement.enricher.tagger import GraphTagger

# from relevation import get_elevation

config = RouteConfig(
    start_lat=50.969540,
    start_lon=-1.383318,
    max_distance=10,
    route_mode="hilly",
    dist_mode="metric",
    elevation_interval=10,
    max_candidates=16000,
    max_condense_passes=5,
)

enricher = GraphEnricher(
    "/home/ross/repos/refinement/data/hampshire-latest.json", config
)
# enricher.enrich_graph()

node_elevations = pd.read_parquet("./data/node_elevations")
node_elevations = [
    (node_id, elevation)
    for node_id, elevation in node_elevations["elevation"].items()
]

tagger = GraphTagger(enricher.graph, config)
tagger.apply_node_elevations(node_elevations)


edges = tagger.get_edge_details()
edges = pd.DataFrame(
    edges,
    columns=[
        "start_id",
        "start_lat",
        "start_lon",
        "end_id",
        "end_lat",
        "end_lon",
    ],
)
edges = edges.assign(bucket=lambda x: x.index // 1000)
edges = edges.set_index(["start_id", "end_id"], drop=True)
edges = pa.Table.from_pandas(edges)

pq.write_to_dataset(
    edges, root_path="./data/edges/", partition_cols=["bucket"]
)
