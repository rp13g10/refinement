import json
import os
from typing import List, Tuple

from bng_latlon import WGS84toOSGB36
from networkx import Graph
from networkx.readwrite import json_graph

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    IntegerType,
)

from refinement.containers import TaggingConfig


class Dumper:

    def __init__(self, source_path: str, config: TaggingConfig):

        self.config = config
        self.graph = self.load_graph(source_path)

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

    def load_graph(self, source_path: str) -> Graph:
        """Read in the contents of the JSON file specified by `source_path`
        to a networkx graph.

        Args:
            source_path (str): The location of the networkx graph to be
              enriched. The graph must have been saved to json format.

        Returns:
            Graph: A networkx graph with the contents of the provided json
              file.
        """

        # Read in the contents of the JSON file
        with open(source_path, "r", encoding="utf8") as fobj:
            graph_data = json.load(fobj)

        # Convert it back to a networkx graph
        graph = json_graph.adjacency_graph(graph_data)

        return graph

    def _get_node_details(self) -> List[Tuple[int, int, int]]:
        """Extracts the node_id, latitude and longitude for each node. Returns
        a list of tuples containing this information.

        Returns:
            List[Tuple[int, int, int]]: A list of tuples containing for each
              node in the graph: id, latitude, longitude
        """

        all_coords = []
        for node_id, node_attrs in self.graph.nodes.items():
            node_lat = node_attrs["lat"]
            node_lon = node_attrs["lon"]
            node_easting, node_northing = WGS84toOSGB36(node_lat, node_lon)
            node_easting_ptn = int(node_easting) // 100
            node_northing_ptn = int(node_northing) // 100
            all_coords.append(
                (
                    node_id,
                    node_lat,
                    node_lon,
                    node_easting_ptn,
                    node_northing_ptn,
                )
            )

        return all_coords

    def _store_raw_nodes(self):
        """Fetches a spark dataframe containing the id, lat and lon for each
        node in the internal graph.

        Returns:
            DataFrame: A spark dataframe containing id, lat and lon columns
        """
        node_list = self._get_node_details()
        node_schema = StructType(
            [
                StructField("id", LongType()),
                StructField("lat", DoubleType()),
                StructField("lon", DoubleType()),
                StructField("easting_ptn", IntegerType()),
                StructField("northing_ptn", IntegerType()),
            ]
        )

        nodes_df = self.sql.createDataFrame(data=node_list, schema=node_schema)

        nodes_df.write.mode("overwrite").partitionBy(
            "easting_ptn", "northing_ptn"
        ).parquet(os.path.join(self.config.data_dir, "raw_nodes"))

    def _get_edge_details(
        self,
    ) -> List[Tuple[int, float, float, int, float, float, str]]:
        """Extracts the start and end points for each edge in the internal
        graph, returns both their IDs and lat/lon coordinates as a tuple
        for each edge in the graph.

        Returns:
            List[Tuple[int, float, float, int, float, float]]: A list in which
              each tuple contains: start_id, src_lat, src_lon, end_id,
              dst_lat, dst_lon
        """

        all_edges = []
        for start_id, end_id in self.graph.edges():
            edge_type = self.graph[start_id][end_id].get("highway")
            start_lat, start_lon = self.fetch_node_coords(start_id)
            end_lat, end_lon = self.fetch_node_coords(end_id)
            start_easting, start_northing = WGS84toOSGB36(start_lat, start_lon)
            easting_ptn = int(start_easting) // 100
            northing_ptn = int(start_northing) // 100
            all_edges.append(
                (
                    start_id,
                    end_id,
                    start_lat,
                    start_lon,
                    end_lat,
                    end_lon,
                    edge_type,
                    easting_ptn,
                    northing_ptn,
                )
            )

        return all_edges  # type: ignore

    def _store_raw_edges(self):
        """Fetches a spark dataframe containing the ID and lat/lon coordinates
        for the start & end point of each edge in the internal graph.

        Returns:
            DataFrame: A spark dataframe containing: start_id, src_lat,
              src_lon, end_id, dst_lat, dst_lon
        """
        edge_list = self._get_edge_details()
        edge_schema = StructType(
            [
                StructField("src", LongType()),
                StructField("dst", LongType()),
                StructField("src_lat", DoubleType()),
                StructField("src_lon", DoubleType()),
                StructField("dst_lat", DoubleType()),
                StructField("dst_lon", DoubleType()),
                StructField("type", StringType()),
                StructField("easting_ptn", IntegerType()),
                StructField("northing_ptn", IntegerType()),
            ]
        )
        edge_df = self.sql.createDataFrame(data=edge_list, schema=edge_schema)

        edge_df.write.mode("overwrite").partitionBy(
            "easting_ptn", "northing_ptn"
        ).parquet(os.path.join(self.config.data_dir, "raw_edges"))

    def store_raw_graph_to_disk(self):

        self._store_raw_nodes()
        self._store_raw_edges()

        del self.graph
