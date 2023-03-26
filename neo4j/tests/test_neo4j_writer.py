import pandas as pd
from unittest.mock import Mock
from utils.neo4j_writer import Neo4jWriter

def test_process_chunk():
    data = {
        "source": [1, 2, 3],
        "target": [2, 3, 4],
        "relationship_type": ["connected", "connected", "connected"]
    }
    df = pd.DataFrame(data)

    # Mock the Neo4jWriter.graph.begin() method to avoid connecting to a real Neo4j instance
    with Mock() as mock_graph_begin:
        neo4j_writer = Neo4jWriter("mock_uri", "mock_user", "mock_password")
        neo4j_writer.graph.begin = mock_graph_begin

