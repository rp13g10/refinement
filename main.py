"""Primary processing script, triggers the conversion of raw OSM data into
a networkx graph"""

from dask.distributed import Client, progress
import dask.bag as db
from tqdm.contrib.concurrent import process_map

from refinement.containers import RouteConfig
from refinement.enricher import GraphEnricher
from refinement.enricher.tagger import (
    GraphTagger,
    _get_elevation_star,
    _get_distance_and_elevation_change_star,
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

if __name__ == "__main__":
    client = Client(n_workers=4)
    node_bag = db.from_sequence(
        nodes,
    )
    elevations = node_bag.map(_get_elevation_star).compute()
    tagger.apply_node_elevations(elevations)


# tagger = GraphTagger(enricher.graph, config)
# nodes = tagger.get_node_details()
# lat = nodes[0][1]
# lon = nodes[0][2]
# get_elevation(lat, lon)

# TODO: Try using standard process_map on nodes/edges to scrape elevation,
#       iterate over chunks of the exploded graph if memory usage is too high.
#       Consider setting number of chunks according to count of nodes & edges
#       to help deal with larger graphs in future.

# if __name__ == "__main__":
#     enricher.enrich_graph(
#         full_target_loc="/home/ross/repos/refinement/data/hampshire-latest-full.nx",
#         cond_target_loc="/home/ross/repos/refinement/data/hampshire-latest-cond.nx",
#     )
