"""Contains the GraphEnricher class, used to add additional data points
to the provided OSM graph."""

import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
)

from relevation import get_elevation, get_distance_and_elevation_change

from refinement.containers import TaggingConfig

# TODO: Investigate whether using graphframes helps to speed up the tagging
#       process. Filtering edges is currently an expensive operation.


class GraphEnricher:
    """Class which enriches the data which is provided by Open Street Maps.
    Unused data is stripped out, and elevation data is added for both nodes and
    edges. The graph itself is condensed, with nodes that lead to dead ends
    or only represent a bend in the route being removed.
    """

    def __init__(
        self,
        config: TaggingConfig,
    ):
        """Create an instance of the graph enricher class based on the
        contents of the networkx graph specified by `source_path`

        Args:
            source_path (str): The location of the networkx graph to be
              enriched. The graph must have been saved to json format.
            config (RouteConfig): A configuration file detailing the route
              requested by the user.
        """

        # Store down core attributes
        self.config = config

        # Generate internal sparkcontext
        conf = SparkConf()
        conf = conf.setAppName("refinement")
        conf = conf.setMaster("local[10]")
        conf = conf.set("spark.driver.memory", "2g")

        sc = SparkContext(conf=conf)
        sc.setLogLevel("WARN")
        self.sc = sc.getOrCreate()
        self.sql = SQLContext(self.sc)

    def enrich_nodes(self):
        """Generates a parquet dataset containing for each node ID, the
        elevation as calculated by the relevation package.
        """

        node_df = self.sql.read.parquet(
            os.path.join(self.config.data_dir, "raw_nodes")
        )

        no_partitions = node_df.count() // 10000
        node_df = node_df.repartition(no_partitions)

        get_elevation_udf = F.udf(get_elevation, returnType=DoubleType())

        node_df = node_df.withColumn(
            "elevation", get_elevation_udf("lat", "lon")
        )

        node_df = node_df.dropna(subset="elevation")

        node_df = node_df.select("node_id", "lat", "lon", "elevation")

        node_df.write.mode("overwrite").parquet(
            os.path.join(self.config.data_dir, "enriched_nodes")
        )

    def _filter_edges_by_nodes(self, edges: DataFrame, nodes: DataFrame):

        start_flags = nodes.select(
            F.col("node_id").alias("start_id"), F.lit(True).alias("start_flag")
        )
        end_flags = nodes.select(
            F.col("node_id").alias("end_id"), F.lit(True).alias("end_flag")
        )

        edges = edges.join(start_flags, on="start_id", how="left")
        edges = edges.join(end_flags, on="end_id", how="left")

        edges = edges.filter(F.col("start_flag") & F.col("end_flag"))

        edges = edges.drop("start_flag", "end_flag")

        return edges

    def enrich_edges(self):
        """Generates a parquet dataset containing the distance change,
        elevation gain and elevation loss for each edge in the internal graph
        """

        edge_df = self.sql.read.parquet(
            os.path.join(self.config.data_dir, "raw_edges")
        )

        node_df = self.sql.read.parquet(
            os.path.join(self.config.data_dir, "enriched_nodes")
        )

        edge_df = self._filter_edges_by_nodes(edge_df, node_df)
        no_partitions = edge_df.count() // 10000
        edge_df = edge_df.repartition(no_partitions)

        get_distance_and_elevation_change_udf = F.udf(
            get_distance_and_elevation_change,
            returnType=StructType(
                [
                    StructField("distance", DoubleType()),
                    StructField("elevation_gain", DoubleType()),
                    StructField("elevation_loss", DoubleType()),
                ]
            ),
        )

        edge_df = edge_df.withColumn(
            "changes",
            get_distance_and_elevation_change_udf(
                "start_lat", "start_lon", "end_lat", "end_lon"
            ),
        )

        edge_df = edge_df.select(
            "start_id",
            "end_id",
            F.col("changes.distance").alias("distance"),
            F.col("changes.elevation_gain").alias("elevation_gain"),
            F.col("changes.elevation_loss").alias("elevation_loss"),
            "type",
        )

        edge_df = edge_df.dropna(
            subset=["elevation_gain", "elevation_loss"], how="any"
        )

        edge_df = edge_df.write.parquet(
            os.path.join(self.config.data_dir, "enriched_edges")
        )

    def enrich_graph(self):
        """Process the provided graph, tagging all nodes and edges with
        elevation data. This requires that a cassandra database with LIDAR data
        available be running."""
        # self.tagger.enrich_nodes()
        self.enrich_edges()
