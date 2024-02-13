"""Contains the GraphEnricher class, used to add additional data points
to the provided OSM graph."""

import json

from networkx import Graph
from networkx.readwrite import json_graph


from refinement.containers import TaggingConfig
from refinement.enricher.tagger import GraphTagger


class GraphEnricher:
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

        self.tagger = GraphTagger(graph, self.config)

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
        # self.tagger.enrich_nodes()
        self.tagger.enrich_edges()
