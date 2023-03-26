from py2neo import Graph, Node, Relationship

class Neo4jWriter:
    def __init__(self, uri, user, password):
        self.graph = Graph(uri, auth=(user, password))

    def process_chunk(self, chunk):
        nodes = []
        relationships = []

        for index, row in chunk.iterrows():
            source_node = Node("Source", id=row['source'])
            target_node = Node("Target", id=row['target'])
            relationship = Relationship(source_node, row['relationship_type'], target_node)

            nodes.extend([source_node, target_node])
            relationships.append(relationship)

        with self.graph.begin() as tx:
            for node in nodes:
                tx.merge(node)

            for relationship in relationships:
                tx.merge(relationship)

        return len(chunk)
