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

tagger = GraphTagger(enricher.graph, config)

nodes = tagger.get_node_details()
nodes = pd.DataFrame(nodes, columns=["node_id", "lat", "lon"])
nodes = nodes.assign(bucket=lambda x: x.index // 10000)
nodes = nodes.set_index("node_id", drop=True)
nodes = pa.Table.from_pandas(nodes)

pq.write_to_dataset(
    nodes, root_path="./data/nodes/", partition_cols=["bucket"]
)
