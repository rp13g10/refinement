"""Primary processing script, triggers the conversion of raw OSM data into
a networkx graph"""

from refinement.containers import TaggingConfig
from refinement.enricher import GraphEnricher

config = TaggingConfig(
    data_dir="/home/ross/repos/refinement/data",
    elevation_interval=10,
    max_condense_passes=5,
)

# TODO: Set tagger as attribute of enricher, or vice-versa

enricher = GraphEnricher(
    "/home/ross/repos/refinement/data/hampshire-latest.json", config
)
# enricher.enrich_graph()

enricher.enrich_graph()

enricher.save_graph("/home/ross/repos/refinement/data/full_graph.nx")

enricher.condense_graph()

enricher.save_graph("/home/ross/repos/refinement/data/condensed_graph.nx")
