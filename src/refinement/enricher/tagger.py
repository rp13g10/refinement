"""Contains the GraphTagger class, which enriches OSM data with elevation data
for both nodes and edges.

Depending on the amount of data which has been loaded into the relevation db,
this can be an extremely heavy script. It is however set up to scale, so
should be well suited to containerised execution."""

import os
from math import ceil
from typing import List, Tuple

import pandas as pd
import pickle
from networkx import Graph
from pyspark.sql import SparkSession, SQLContext, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

from relevation import get_elevation, get_distance_and_elevation_change

from refinement.containers import TaggingConfig
from refinement.graph_utils.route_helper import RouteHelper


class GraphTagger(RouteHelper):
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
        super().__init__(graph, config)

        # Generate internal sparkcontext
        self.sc = (
            SparkSession.builder.appName("refinement")  # type: ignore
            .master("local[10]")
            .getOrCreate()
        )

        self.sql = SQLContext(self.sc)

    def _get_node_details(self) -> List[Tuple[int, int, int]]:
        """Extracts the node_id, latitude and longitude for each node. Returns
        a list of tuples containing this information.

        Returns:
            List[Tuple[int, int, int]]: A list of tuples containing for each
              node in the graph: id, latitude, longitude
        """

        all_coords = [
            (node_id, node_attrs["lat"], node_attrs["lon"])
            for node_id, node_attrs in self.graph.nodes.items()
        ]
        return all_coords

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

    def _get_node_df(self) -> DataFrame:
        """Fetches a spark dataframe containing the id, lat and lon for each
        node in the internal graph.

        Returns:
            DataFrame: A spark dataframe containing id, lat and lon columns
        """
        node_list = self._get_node_details()
        node_schema = StructType(
            [
                StructField("node_id", LongType()),
                StructField("lat", DoubleType()),
                StructField("lon", DoubleType()),
            ]
        )

        nodes_df = self.sql.createDataFrame(data=node_list, schema=node_schema)
        nodes_df = nodes_df.repartition(ceil(len(node_list)/10000))

        return nodes_df

    def _precompute_node_elevations(self):
        """Generates a parquet dataset containing for each node ID, the
        elevation as calculated by the relevation package.
        """
        node_df = self._get_node_df()

        _get_elevation_udf = F.udf(get_elevation, returnType=DoubleType())

        node_df = node_df.withColumn(
            "elevation", _get_elevation_udf("lat", "lon")
        )

        node_df = node_df.select("node_id", "elevation")

        node_df.write.parquet(
            os.path.join(self.config.data_dir, "node_elevations")
        )

    def _apply_node_elevations(self):
        """Reads in the contents of the precomputed node elevations dataset
        and applies the elevation data to the internal graph.
        """

        elevations_df = pd.read_parquet(
            os.path.join(self.config.data_dir, "node_elevations")
        )
        elevations_df = elevations_df.set_index("node_id", drop=True)
        elevations = elevations_df["elevation"].to_dict()
        del elevations_df

        to_delete = set()
        for node_id, elevation in elevations.items():
            if pd.notna(elevation):
                self.graph.nodes[node_id]["elevation"] = elevation
            else:
                to_delete.add(node_id)

        for node_id in self.graph.nodes:
            node_data = self.graph.nodes[node_id]
            if 'elevation' not in node_data:
                to_delete.add(node_id)
            elif 'lat' not in node_data:
                to_delete.add(node_id)
            elif 'lon' not in node_data:
                to_delete.add(node_id)

        # Remove nodes with no elevation data
        self.graph.remove_nodes_from(to_delete)

    def tag_nodes(self):
        """Enriches all nodes in the internal graph with elevation data, if
        elevation data for a node is not present (i.e. the relevant file has
        not yet been ingested into the backend database for the relevation
        package) then the node will be removed from the graph.
        """
        try:
            self.load_graph("graph_with_nodes.nx")
        except FileNotFoundError:
            # self._precompute_node_elevations()
            self._apply_node_elevations()
            self.store_graph("graph_with_nodes.nx")

    def _get_edge_details(
        self,
    ) -> List[Tuple[int, float, float, int, float, float]]:
        """Extracts the start and end points for each edge in the internal
        graph, returns both their IDs and lat/lon coordinates as a tuple
        for each edge in the graph.

        Returns:
            List[Tuple[int, float, float, int, float, float]]: A list in which
              each tuple contains: start_id, start_lat, start_lon, end_id,
              end_lat, end_lon
        """
        all_edges = (
            (
                start_id,
                self.fetch_node_coords(start_id),
                end_id,
                self.fetch_node_coords(end_id),
            )
            for start_id, end_id in self.graph.edges()
        )
        all_edges = [
            (start_id, start_lat, start_lon, end_id, end_lat, end_lon)
            for start_id, (start_lat, start_lon), end_id, (
                end_lat,
                end_lon,
            ) in all_edges
        ]
        return all_edges  # type: ignore

    def _get_edge_df(self) -> DataFrame:
        """Fetches a spark dataframe containing the ID and lat/lon coordinates
        for the start & end point of each edge in the internal graph.

        Returns:
            DataFrame: A spark dataframe containing: start_id, start_lat,
              start_lon, end_id, end_lat, end_lon
        """
        edge_list = self._get_edge_details()
        edge_schema = StructType(
            [
                StructField("start_id", LongType()),
                StructField("start_lat", DoubleType()),
                StructField("start_lon", DoubleType()),
                StructField("end_id", LongType()),
                StructField("end_lat", DoubleType()),
                StructField("end_lon", DoubleType()),
            ]
        )
        edge_df = self.sql.createDataFrame(data=edge_list, schema=edge_schema)
        edge_df = edge_df.repartition(ceil(len(edge_list)/10000))

        return edge_df

    def _precompute_edge_elevations(self):
        """Generates a parquet dataset containing the distance change,
        elevation gain and elevation loss for each edge in the internal graph
        """
        edge_df = self._get_edge_df()

        _get_distance_and_elevation_change_udf = F.udf(
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
            _get_distance_and_elevation_change_udf(
                "start_lat", "start_lon", "end_lat", "end_lon"
            ),
        )

        edge_df = edge_df.select(
            "start_id",
            "end_id",
            F.col("changes.distance").alias("distance"),
            F.col("changes.elevation_gain").alias("elevation_gain"),
            F.col("changes.elevation_loss").alias("elevation_loss"),
        )

        edge_df = edge_df.write.parquet(
            os.path.join(self.config.data_dir, "edge_elevations")
        )

    def _apply_edge_changes(self):
        """Reads in the contents of the precomputed edge elevations dataset
        and applies the data to the internal graph."""

        edge_changes_df = pd.read_parquet(
            os.path.join(self.config.data_dir, "edge_elevations")
        )

        for _, row in edge_changes_df.iterrows():
            start_id = row["start_id"]  # type: ignore
            end_id = row["end_id"]  # type: ignore
            dist_change = row["distance"]  # type: ignore
            elevation_gain = row["elevation_gain"]  # type: ignore
            elevation_loss = row["elevation_loss"]  # type: ignore

            data = self.graph[start_id][end_id]

            data["distance"] = dist_change
            data["elevation_gain"] = elevation_gain
            data["elevation_loss"] = elevation_loss
            data["via"] = []

            # Clear out any other attributes which aren't needed
            to_remove = [
                attr
                for attr in data
                if attr
                not in {"distance", "elevation_gain", "elevation_loss", "via"}
            ]
            for attr in to_remove:
                del data[attr]

        # to_remove = set()
        # for start_id, end_id in self.graph.edges():
        #     data = self.graph[start_id][end_id]
        #     if "distance" not in data:
        #         to_remove.add((start_id, end_id))
        # self.graph.remove_edges_from(to_remove)

    def tag_edges(self):
        """Enriches all of the edges in the internal graph with distances,
        elevation gains and elevation losses. This should not be called
        until tag_nodes has already been run."""
        try:
            self.load_graph("graph_with_edges.nx")
        except FileNotFoundError:
            # self._precompute_edge_elevations()
            self._apply_edge_changes()
            self.store_graph("graph_with_edges.nx")
