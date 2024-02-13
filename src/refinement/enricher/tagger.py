"""Contains the GraphTagger class, which enriches OSM data with elevation data
for both nodes and edges.

Depending on the amount of data which has been loaded into the relevation db,
this can be an extremely heavy script. It is however set up to scale, so
should be well suited to containerised execution."""

import os
from typing import Tuple

import pickle
from networkx import Graph
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
)

from relevation import get_elevation, get_distance_and_elevation_change

from refinement.containers import TaggingConfig


class GraphTagger:
    """Class which enriches the data which is provided by Open Street Maps.
    Unused data is stripped out, and elevation data is added for both nodes and
    edges. The graph itself is condensed, with nodes that lead to dead ends
    or only represent a bend in the route being removed.
    """

    def __init__(
        self,
        graph: Graph,
        config: TaggingConfig,
    ):
        """Create an instance of the graph enricher class based on the
        contents of the networkx graph specified by `source_path`

        Args:
            graph (Graph): The network graph to be enriched with elevation data
            config (TaggingConfig): User configuration options
        """

        # Store down core attributes
        self.graph = graph
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

    def fetch_node_coords(self, node_id: int) -> Tuple[int, int]:
        """Convenience function, retrieves the latitude and longitude for a
        single node in a graph."""
        node = self.graph.nodes[node_id]
        lat = node["lat"]
        lon = node["lon"]
        return lat, lon

    def store_graph(self, fname: str):
        """Convenience function which will pickle the internal graph to the
        specified location.

        Args:
            fname (str): The location where the internal graph should be saved,
              relative to the configured data directory.
        """
        with open(os.path.join(self.config.data_dir, fname), "wb") as fobj:
            pickle.dump(self.graph, fobj)

    def load_graph(self, fname: str):
        """Convenience function which will unpickle the internal graph from
        the specified location.

        Args:
            fname (str): The location where the internal graph should be
              loaded from, relative to the configured data directory
        """
        with open(os.path.join(self.config.data_dir, fname), "rb") as fobj:
            graph = pickle.load(fobj)
        self.graph = graph

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
