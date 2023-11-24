"""Contains the GraphTagger class, which will be executed if this script
is executed directly."""

from itertools import repeat

import networkx as nx
import pandas as pd
from dask.distributed import Client
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map
from networkx import Graph
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module

from relevation import get_elevation, get_distance_and_elevation_change

from refinement.containers import RouteConfig
from refinement.graph_utils.route_helper import RouteHelper
from refinement.graph_utils.splitter import GraphSplitter


class GraphTagger(RouteHelper):
    """Class which enriches the data which is provided by Open Street Maps.
    Unused data is stripped out, and elevation data is added for both nodes and
    edges. The graph itself is condensed, with nodes that lead to dead ends
    or only represent a bend in the route being removed.
    """

    def __init__(
        self,
        graph: Graph,
        config: RouteConfig,
    ):
        """Create an instance of the graph enricher class based on the
        contents of the networkx graph specified by `source_path`

        Args:
            source_path (str): The location of the networkx graph to be
              enriched. The graph must have been saved to json format.
            dist_mode (str, optional): The preferred output mode for distances
              which are saved to node edges. Returns kilometers if set to
              metric, miles if set to imperial. Defaults to "metric".
            elevation_interval (int, optional): When calculating elevation
              changes across an edge, values will be estimated by taking
              checkpoints at regular checkpoints. Smaller values will result in
              more accurate elevation data, but may slow down the calculation.
              Defaults to 10.
            max_condense_passes (int, optional): When condensing the graph, new
              dead ends may be created on each pass (i.e. if one dead end
              splits into two, pass 1 removes the 2 dead ends, pass 2 removes
              the one they split from). Use this to set the maximum number of
              passes which will be performed.
        """

        # Store down core attributes
        super().__init__(graph, config)

    def get_node_details(self):
        all_coords = [
            (node_id, node_attrs["lat"], node_attrs["lon"])
            for node_id, node_attrs in self.graph.nodes.items()
        ]
        return all_coords

    def apply_node_elevations(self, elevations):
        to_delete = set()
        for node_id, elevation in elevations:
            if pd.notna(elevation):
                self.graph.nodes[node_id]["elevation"] = elevation
            else:
                to_delete.add(node_id)

        # Remove nodes with no elevation data
        self.graph.remove_nodes_from(to_delete)

    def get_edge_details(self):
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

    def apply_edge_changes(self, edge_changes):
        for (
            start_id,
            end_id,
            dist_change,
            elevation_gain,
            elevation_loss,
        ) in edge_changes:
            data = self.graph[start_id][end_id]

            if self.config.dist_mode == "metric":
                dist_change = dist_change.kilometers
            else:
                dist_change = dist_change.miles

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


# def tag_graph(graph: Graph, config: RouteConfig):
#     # Split the graph across a grid
#     splitter = GraphSplitter(graph)
#     splitter.explode_graph()

#     pbar = tqdm(total=len(splitter.subgraphs) + 1)
#     for (lat_inx, lon_inx), subgraph in splitter.subgraphs.items():
#         pbar.set_description(f"{lat_inx}:{lon_inx}")
#         sub_tagger = GraphTagger(subgraph, config)

#         sub_nodes = sub_tagger.get_node_details()
#         sub_elevations = get_node_elevations(sub_nodes)
#         sub_tagger.apply_node_elevations(sub_elevations)

#         sub_edges = sub_tagger.get_edge_details()
#         sub_edge_changes = get_edge_changes(sub_edges)
#         sub_tagger.apply_edge_changes(sub_edge_changes)

#         pbar.update(1)

#     pbar.set_description("Mopup")
#     splitter.rebuild_graph()
#     mopup_tagger = GraphTagger(splitter.graph, config)

#     mopup_nodes = mopup_tagger.get_node_details()
#     mopup_elevations = get_node_elevations(mopup_nodes)
#     mopup_tagger.apply_node_elevations(mopup_elevations)

#     mopup_edges = mopup_tagger.get_edge_details()
#     mopup_edge_changes = get_edge_changes(mopup_edges)
#     mopup_tagger.apply_edge_changes(mopup_edge_changes)

#     pbar.update(1)
#     pbar.close()

#     return mopup_tagger.graph
