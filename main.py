"""Primary processing script, triggers the conversion of raw OSM data into
a networkx graph.

This is a very heavy script. If relevation has been seeded with more than
~10 LIDAR files it is recommended that it be run on either a high-end
desktop, or a reasonably provisioned EC2 cluster."""

import os

from refinement.containers import TaggingConfig
from refinement.enricher import GraphEnricher

# TODO: Evaluate memory footprint of networkx graph, if necessary look at
#       ways to parallelize it

config = TaggingConfig(
    data_dir="/home/ross/repos/refinement/data",
    elevation_interval=10,
    max_condense_passes=5,
)

enricher = GraphEnricher(
    os.path.join(config.data_dir, "hampshire-latest.json"), config
)
enricher.enrich_graph()

# NOTE: Need to confirm that enricher.graph and tagger.graph are indeed
#       exactly the same object

# enricher.enrich_graph()
# enricher.tagger._prepare_nodes()
# enricher.tagger._precompute_node_elevations()
# enricher.tagger._apply_node_elevations()
# enricher.store_graph('graph_with_nodes.nx')

enricher.save_graph(os.path.join(config.data_dir, "full_graph.nx"))

enricher.condense_graph()

enricher.save_graph(os.path.join(config.data_dir, "condensed_graph.nx"))


# TODO: Figure out how to structure this properly for execution with Dash
