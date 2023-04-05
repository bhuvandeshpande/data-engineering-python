from reader import CsvReader
from neo4j import GraphDatabase
import pandas as pd
import multiprocessing

class MultiProcessor():

    def read_file(self, args):
        file, schemas, ds = args
        print(f'Reading {ds} {file} file in parallel')
        cv = CsvReader(file, schemas, ds)
        df_reader = cv.read_csv_file()
        return df_reader

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
    

    def split_dataframe(self, df, chunk_size=10000):
    # Split the dataframe into smaller chunks
        return [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]

    # Modify the process_file_neo4j method in the MultiProcessor class        
    def process_file_neo4j(self, file, schemas, ds, db_conn):
        print(f'Starting to process: {ds} {file} file')

        uri = db_conn['uri']
        driver = GraphDatabase.driver(uri, auth=(db_conn['user'], db_conn['password']))

        # Create and set the Neo4j database
        neo_driver = self.create_and_set_neo4j_database(driver, ds)
        
        # Use multiprocessing for reading the file in parallel
        with multiprocessing.Pool() as read_pool:
            df_reader_list = read_pool.map(self.read_file, [(file, schemas, ds)])

        # Since read_csv_file returns a combined DataFrame, use the first element of df_reader_list
        combined_df = df_reader_list[0]

        # Split the DataFrame into chunks
        df_chunks = self.split_dataframe(combined_df, chunk_size=10000)

        # Use multiprocessing for writing the dataframes to Neo4j in parallel
        with multiprocessing.Pool() as write_pool:
            write_pool.map(self.write_chunk_neo4j, [(chunk, neo_driver, ds, idx) for idx, chunk in enumerate(df_chunks)])

        driver.close()
        print(f'Finished processing file: {file}')  
