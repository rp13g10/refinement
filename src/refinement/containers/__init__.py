from typing import List, Set, Optional, Tuple
from dataclasses import dataclass

# TODO: Document the arguments for these classes properly
# TODO: Check whether slots can be used to minimise memory footprint of these
#       classes


@dataclass
class TaggingConfig:
    """Contains user configuration options for the tagging of a network
    graph with elevation data

    Args:
        data_dir (str): The absolute path of a folder which will be used to
          house temporary data files while tagging is in progress.
        elevation_interval (int): When estimating elevation gain between
          points, this determines how frequently the elevation should be
          sampled. Lower values will give more accurate elevation gain/loss
          metrics, higher values will result in faster script execution.
        max_condense_passes (int): Determines how many times the internal map
          should be processed to minimise its size. Decreasing this may improve
          processing times, but the benefit is likely to be negligible.
    """

    data_dir: str
    elevation_interval: int = 10
    max_condense_passes: int = 5


@dataclass
class BBox:
    """Contains information about the physical boundaries of one or more
    routes

    Args:
        min_lat (float): Minimum latitude
        min_lon (float): Minimum longitude
        max_lat (float): Maximum latitude
        max_lon (float): Maximum longitude"""

    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float
