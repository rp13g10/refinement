"""Contains the GraphTagger class, which will be executed if this script
is executed directly."""

import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pickle
from dask.distributed import Client
import dask.dataframe as dd
from networkx import Graph

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

    def _get_node_details(self):
        all_coords = [
            (node_id, node_attrs["lat"], node_attrs["lon"])
            for node_id, node_attrs in self.graph.nodes.items()
        ]
        return all_coords

    def store_graph(self, fname: str):
        with open(os.path.join(self.config.data_dir, fname), "wb") as fobj:
            pickle.dump(self.graph, fobj)

    def _cache_graph(self):
        self.store_graph("_cached_graph.nx")
        del self.graph

    def load_graph(self, fname: str):
        with open(os.path.join(self.config.data_dir, fname), "rb") as fobj:
            graph = pickle.load(fobj)
        self.graph = graph

    def _uncache_graph(self):
        self.load_graph("_cached_graph.nx")

    def _prepare_nodes(self):
        nodes = self._get_node_details()
        self._cache_graph()

        nodes = pd.DataFrame(nodes, columns=["node_id", "lat", "lon"])
        nodes = nodes.assign(bucket=lambda x: x.index // 10000)
        nodes = nodes.set_index("node_id", drop=True)
        nodes = pa.Table.from_pandas(nodes)

        pq.write_to_dataset(
            nodes,
            root_path=os.path.join(self.config.data_dir, "nodes"),
            partition_cols=["bucket"],
        )
        # self._uncache_graph()

    @staticmethod
    def _get_elevation_for_row(row):
        return get_elevation(row.lat, row.lon)

    def _precompute_node_elevations(self):
        # self._cache_graph()
        client = Client()
        nodes_df = dd.read_parquet(  # type: ignore
            os.path.join(self.config.data_dir, "nodes")
        )
        elevations = nodes_df.apply(  # type: ignore
            self._get_elevation_for_row,
            axis=1,
            meta=("elevation", "float"),
        )
        nodes_df = nodes_df.assign(elevation=elevations)  # type: ignore
        nodes_df.to_parquet(
            os.path.join(self.config.data_dir, "node_elevations"),
            partition_on=["bucket"],
        )
        client.close()
        # self._uncache_graph()

    def _apply_node_elevations(self):
        self._uncache_graph()
        elevations_df = pd.read_parquet(
            os.path.join(self.config.data_dir, "node_elevations")
        )
        elevations = elevations_df["elevation"].to_dict()
        del elevations_df

        to_delete = set()
        for node_id, elevation in elevations:
            if pd.notna(elevation):
                self.graph.nodes[node_id]["elevation"] = elevation
            else:
                to_delete.add(node_id)

        # Remove nodes with no elevation data
        self.graph.remove_nodes_from(to_delete)

    def tag_nodes(self):
        try:
            self.load_graph("graph_with_nodes.nx")
        except FileNotFoundError:
            self._prepare_nodes()
            self._precompute_node_elevations()
            self._apply_node_elevations()
            self.store_graph("graph_with_nodes.nx")

    def _prepare_edges(self):
        edges = self._get_edge_details()
        self._cache_graph()

        edges = pd.DataFrame(
            edges,
            columns=[
                "start_id",
                "start_lat",
                "start_lon",
                "end_id",
                "end_lat",
                "end_lon",
            ],
        )
        edges = edges.assign(bucket=lambda x: x.index // 1000)
        edges = edges.set_index(["start_id", "end_id"], drop=True)
        edges = pa.Table.from_pandas(edges)

        pq.write_to_dataset(
            edges,
            root_path=os.path.join(  # type: ignore
                self.config.data_dir, "edges", partition_cols=["bucket"]
            ),
        )
        self._uncache_graph()

    @staticmethod
    def get_edge_distance_and_elevation_for_row(row):
        return get_distance_and_elevation_change(
            row.start_lat, row.start_lon, row.end_lat, row.end_lon
        )

    def _precompute_edge_elevations(self):
        self._cache_graph()
        client = Client()
        edges_df = dd.read_parquet(  # type: ignore
            os.path.join(self.config.data_dir, "edges")
        )
        edge_elevations = edges_df.apply(  # type: ignore
            self.get_edge_distance_and_elevation_for_row,
            axis=1,
            meta={0: "float", 1: "float", 2: "float"},
            result_type="expand",
        )
        edge_elevations.columns = [
            "dist_change",
            "elevation_gain",
            "elevation_loss",
        ]
        edge_elevations = (
            edge_elevations.assign(start_id=edges_df.start_id)  # type: ignore
            .assign(end_id=edges_df.end_id)  # type: ignore
            .assign(bucket=edges_df.bucket)  # type: ignore
        )

        edge_elevations.to_parquet(
            os.path.join(self.config.data_dir, "edge_elevations"),
            partition_on=["bucket"],
        )
        client.close()
        self._uncache_graph()

    def _get_edge_details(self):
        all_edges = [
            (
                start_id,
                self.fetch_node_coords(start_id),
                end_id,
                self.fetch_node_coords(end_id),
            )
            for start_id, end_id in self.graph.edges()
        ]
        all_edges = [
            (start_id, start_lat, start_lon, end_id, end_lat, end_lon)
            for start_id, (start_lat, start_lon), end_id, (
                end_lat,
                end_lon,
            ) in all_edges
        ]
        return all_edges

    def _apply_edge_changes(self):
        edge_changes_df = pd.read_parquet(
            os.path.join(self.config.data_dir, "edge_elevations")
        )

        for row in edge_changes_df.iterrows():
            start_id = row["start_id"]  # type: ignore
            end_id = row["end_id"]  # type: ignore
            dist_change = row["dist_change"]  # type: ignore
            elevation_gain = row["elevation_gain"]  # type: ignore
            elevation_loss = row["elevation_loss"]  # type: ignore

            data = self.graph[start_id][end_id]

            dist_change = dist_change.kilometers

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

    def tag_edges(self):
        try:
            self.load_graph("graph_with_edges.nx")
        except FileNotFoundError:
            self._prepare_edges()
            self._precompute_edge_elevations()
            self._apply_edge_changes()
            self.store_graph("graph_with_edges.nx")
