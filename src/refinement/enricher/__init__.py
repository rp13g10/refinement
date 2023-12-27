"""Contains the GraphEnricher class, used to add additional data points
to the provided OSM graph."""

import json
import pickle

from networkx import Graph
from networkx.readwrite import json_graph


from refinement.containers import TaggingConfig
from refinement.graph_utils.route_helper import RouteHelper
from refinement.enricher.tagger import GraphTagger
from refinement.graph_utils.condenser import condense_graph


class GraphEnricher(RouteHelper):
    """Class which enriches the data which is provided by Open Street Maps.
    Unused data is stripped out, and elevation data is added for both nodes and
    edges. The graph itself is condensed, with nodes that lead to dead ends
    or only represent a bend in the route being removed.
    """

    def __init__(
        self,
        source_path: str,
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

        # Store down user preferences
        self.config = config

        # Read in the contents of the graph
        graph = self.load_graph(source_path)

        # Store down core attributes
        super().__init__(graph, config)

        self.tagger = GraphTagger(self.graph, self.config)

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

    def enrich_graph(self):
        """Process the provided graph, tagging all nodes and edges with
        elevation data. This requires that a cassandra database with LIDAR data
        available be running."""
        self.tagger.tag_nodes()
        self.tagger.tag_edges()

    def condense_graph(self):
        """Reduce the size of the internal graph by removing any nodes which
        do not correspond to a junction. Paths/roads will be represented by
        a single edge, rather than a chain of edges. The intermediate nodes
        traversed by each edge will instead be recorded in the 'via' attribute
        of each new edge."""
        self.graph = condense_graph(self.graph)

    def save_graph(self, target_loc: str):
        """Once processed, pickle the graph to the local filesystem ready for
        future use.

        Args:
            target_loc (str): The location which the graph should be saved to.
        """
        with open(target_loc, "wb") as fobj:
            pickle.dump(self.graph, fobj)
