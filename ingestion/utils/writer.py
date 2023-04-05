class Neo4JWriter():
    def write_chunk_neo4j(self, args):
        df, driver, ds, idx = args
        print(f'Writing chunk {idx} with size {df.shape[0]} of {ds} table to Neo4j in parallel')

        # Function to write the data to Neo4j
        def write_data_to_neo4j(tx, query, properties_list):
            tx.run(query, properties_list=properties_list)

        # Prepare the properties list
        properties_list = [row.to_dict() for _, row in df.iterrows()]

        # Define the query to insert data into the Neo4j database
        # Modify the query according to your database schema and data requirements
        query = f"UNWIND $properties_list as properties CREATE (n: {ds}) SET n = properties"

        # Write the data to the Neo4j database
        with driver.session() as session:
            with session.begin_transaction() as tx:
                write_data_to_neo4j(tx, query, properties_list)
                tx.commit()

    # Add this new method to the MultiProcessor class
    def create_and_set_neo4j_database(self, driver, ds):
        # Create a session with the driver
        with driver.session() as session:
            # Run a query to create the database if it doesn't exist
            session.run(f"CREATE DATABASE {ds} IF NOT EXISTS")

        # Create a new driver instance for the newly created database
        uri = f"neo4j://{driver._config.address}"
        new_uri = uri.replace(driver.database, ds)
        new_driver = GraphDatabase.driver(new_uri, auth=driver.auth)

        return new_driver