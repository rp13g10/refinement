"""Primary processing script, triggers the conversion of raw OSM data into
a networkx graph"""

from refinement.containers import RouteConfig
from refinement.enricher import GraphEnricher

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

# TODO: Try using standard process_map on nodes/edges to scrape elevation,
#       iterate over chunks of the exploded graph if memory usage is too high.
#       Consider setting number of chunks according to count of nodes & edges
#       to help deal with larger graphs in future.

enricher.enrich_graph(
    full_target_loc="/home/ross/repos/refinement/data/hampshire-latest-full.nx",
    cond_target_loc="/home/ross/repos/refinement/data/hampshire-latest-cond.nx",
)