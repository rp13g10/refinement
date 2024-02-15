"""Initial pre-processing script, extracts node and edge details from the
provided .json graph. These details are saved down to parquet, so that they
are ready to be tagged with elevation data by spark."""

import os

from refinement.containers import TaggingConfig
from refinement.dumper import Dumper


config = TaggingConfig(
    data_dir="/home/ross/repos/refinement/data",
    elevation_interval=10,
    max_condense_passes=5,
)

dumper = Dumper(os.path.join(config.data_dir, "hampshire-latest.json"), config)

dumper.store_raw_graph_to_disk()
