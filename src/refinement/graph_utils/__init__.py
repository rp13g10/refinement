from typing import Tuple

from geopy.distance import distance
from networkx import Graph


class GraphUtils:
    def __init__(self, graph: Graph):
        self.graph = graph

    def fetch_node_coords(self, node_id: int) -> Tuple[int, int]:
        """Convenience function, retrieves the latitude and longitude for a
        single node in a graph."""
        node = self.graph.nodes[node_id]
        lat = node["lat"]
        lon = node["lon"]
        return lat, lon

    def get_straight_line_distance_and_elevation_change(
        self, start_id: int, end_id: int
    ) -> Tuple[float, float, float]:
        """Calculate the change in elevation accrued when travelling in a
        straight line from one node to another

        Args:
            start_id (int): The ID of the start node
            end_id (int): The ID of the end node

        Returns:
            Tuple[float, float, float]: The distance from start_id to end_id,
              the elevation gain and the elevation loss
        """

        # Elevation change
        start_ele = self.graph.nodes[start_id]["elevation"]
        end_ele = self.graph.nodes[end_id]["elevation"]
        change = end_ele - start_ele
        gain = max(0, change)
        loss = abs(min(0, change))

        # Distance change
        start_lat, start_lon = self.fetch_node_coords(start_id)
        end_lat, end_lon = self.fetch_node_coords(end_id)
        dist = distance((start_lat, start_lon), (end_lat, end_lon))

        dist = dist.kilometers

        return dist, gain, loss
