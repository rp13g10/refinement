import math
from typing import List, Tuple

import numpy as np
from geopy.distance import distance
from networkx import Graph

from relevation import get_elevation

from refinement.containers import TaggingConfig
from refinement.graph_utils import GraphUtils


# TODO: Implement parallel processing for condensing of enriched graphs.


class RouteHelper(GraphUtils):
    """Contains utility functions which are useful when manipulating data which
    relates to the fiding of running routes.
    """

    def __init__(self, graph: Graph, config: TaggingConfig):
        self.graph = graph
        self.config = config

    def _get_elevation_checkpoints(
        self,
        start_lat: float,
        start_lon: float,
        end_lat: float,
        end_lon: float,
    ) -> Tuple[List[float], List[float], distance]:
        """Given a start & end point, return a list of equally spaced latitudes
        and longitudes between them. These can then be used to estimate the
        elevation change between start & end by calculating loss/gain between
        each checkpoint.

        Args:
            start_lat (float): Latitude for the start point
            start_lon (float): Longitude for the start point
            end_lat (float): Latitude for the end point
            end_lon (float): Longitude for the end point

        Returns:
            Tuple[List[float], List[float], distance]: A list of latitudes and
              a corresponding list of longitudes which represent points on an
              edge of the graph. A geopy distance object which shows the total
              distance between the start & end point
        """
        # Calculate distance from A to B
        dist_change = distance((start_lat, start_lon), (end_lat, end_lon))

        # Calculate number of checks required to get elevation every N metres
        dist_change_m = dist_change.meters
        no_checks = math.ceil(dist_change_m / self.config.elevation_interval)
        no_checks = max(2, no_checks)

        # Generate latitudes & longitudes for each checkpoint
        lat_checkpoints = list(
            np.linspace(start_lat, end_lat, num=no_checks, endpoint=True)
        )
        lon_checkpoints = list(
            np.linspace(start_lon, end_lon, num=no_checks, endpoint=True)
        )

        return lat_checkpoints, lon_checkpoints, dist_change

    def _calculate_elevation_change_for_checkpoints(
        self, lat_checkpoints: List[float], lon_checkpoints: List[float]
    ) -> Tuple[float, float]:
        """For the provided latitude/longitude coordinates, estimate the total
        elevation gain/loss along the entire route.

        Args:
            lat_checkpoints (List[float]): A list of equally spaced latitudes
              which represent points on an edge of the graph
            lon_checkpoints (List[float]): A list of equally spaced longitudes
              which represent points on an edge of the graph

        Returns:
            Tuple[float, float]: Elevation gain in metres, elevation loss in
              metres
        """
        # Calculate elevation at each checkpoint
        elevations = []
        for lat, lon in zip(lat_checkpoints, lon_checkpoints):
            elevation = get_elevation(lat, lon)
            elevations.append(elevation)

        # Work out the sum of elevation gains/losses between checkpoints
        last_elevation = None
        elevation_gain = 0.0
        elevation_loss = 0.0
        for elevation in elevations:
            if not last_elevation:
                last_elevation = elevation
                continue
            if elevation > last_elevation:
                elevation_gain += elevation - last_elevation
            elif elevation < last_elevation:
                elevation_loss += last_elevation - elevation
            last_elevation = elevation

        return elevation_gain, elevation_loss

    def _estimate_distance_and_elevation_change(
        self,
        start_lat: float,
        start_lon: float,
        end_lat: float,
        end_lon: float,
    ) -> Tuple[float, float, float]:
        """For a given start & end node, estimate the change in elevation when
        traversing the edge between them. The number of samples used to
        estimate the change in elevation is determined by the
        self.elevation_interval attribute.

        Args:
            start_id (int): The starting node for edge traversal
            end_id (int): The end node for edge traversal

        Returns:
            Tuple[float, float, float]: The distance change, elevation gain
              and elevation loss
        """

        (
            lat_checkpoints,
            lon_checkpoints,
            dist_change,
        ) = self._get_elevation_checkpoints(
            start_lat, start_lon, end_lat, end_lon
        )

        (
            elevation_gain,
            elevation_loss,
        ) = self._calculate_elevation_change_for_checkpoints(
            lat_checkpoints, lon_checkpoints
        )

        dist_change = float(dist_change.km)

        return dist_change, elevation_gain, elevation_loss
