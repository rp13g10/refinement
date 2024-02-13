"""Primary processing script, triggers the conversion of raw OSM data into
a networkx graph.

This is a very heavy script. If relevation has been seeded with more than
~10 LIDAR files it is recommended that it be run on either a high-end
desktop, or a reasonably provisioned EC2 cluster."""

import os

from refinement.containers import TaggingConfig
from refinement.enricher import GraphEnricher

# TODO: Split initial dump to parquet and enrichment process across
#       two scripts. Ensures full graph not in memory while tagging nodes
#       with elevation data.

# TODO: Switch this over to using graphframes
# TODO: Move graph condensing into webapp, after user has selected desired
#       path types.

config = TaggingConfig(
    data_dir="/home/ross/repos/refinement/data",
    elevation_interval=10,
    max_condense_passes=5,
)

enricher = GraphEnricher(
    os.path.join(config.data_dir, "hampshire-latest.json"), config
)
enricher.enrich_graph()
