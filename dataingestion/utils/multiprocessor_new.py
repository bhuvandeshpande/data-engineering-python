from .reader import CsvReader
from .dbwriter import DBWriter
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

    def write_chunk(self, args):
        df, db_conn, ds, idx = args
        print(f'Writing chunk {idx} with size {df.shape[0]} of {ds} table in parallel')
        dbwriter = DBWriter(df, db_conn, ds, idx)
        dbwriter.write_to_postgres()

    def process_file(self, file, schemas, ds, db_conn):
        print(f'Starting to process: {ds} {file} file')

        # Use multiprocessing for reading the file in parallel
        with multiprocessing.Pool() as read_pool:
            df_reader_list = read_pool.map(self.read_file, [(file, schemas, ds)])

        # Use multiprocessing for writing the dataframes in parallel
        with multiprocessing.Pool() as write_pool:
            write_pool.map(self.write_chunk, [(df, db_conn, ds, idx) for idx, df in enumerate(df_reader_list)])

        print(f'Finished processing file: {file}')
        return df_reader_list


    def write_chunk_neo4j(self, args):
        df, uri, user, password, ds, idx = args
        print(f'Writing chunk {idx} with size {df.shape[0]} of {ds} table to Neo4j in parallel')

        # Function to write the data to Neo4j
        def write_data_to_neo4j(tx, query, properties_list):
            tx.run(query, properties_list=properties_list)

        # Connect to the Neo4j database
        driver = GraphDatabase.driver(uri, auth=(user, password))

        # Prepare the properties list
        properties_list = [row.to_dict() for _, row in df.iterrows()]

        # Define the query to insert data into the Neo4j database
        # Modify the query according to your database schema and data requirements
        query = f"UNWIND $properties_list as properties CREATE (n: {ds}) SET n = properties"

        # Write the data to the Neo4j database
        with driver.session() as session:
            session.write_transaction(write_data_to_neo4j, query, properties_list)

        driver.close()
        
    # Add this new method to the MultiProcessor class
    def create_and_set_neo4j_database(self, driver, db_name):
        with driver.session() as session:
            session.run(f"CREATE DATABASE {db_name} IF NOT EXISTS")
            session.run(f"USE {db_name}")

    # Modify the process_file_neo4j method in the MultiProcessor class        
    def process_file_neo4j(self, file, schemas, ds, db_conn):
        print(f'Starting to process: {ds} {file} file')

        uri = db_conn['uri']
        driver = GraphDatabase.driver(uri, auth=(db_conn['user'], db_conn['password']))
        self.create_and_set_neo4j_database(driver, ds)

        # Use multiprocessing for reading the file in parallel
        with multiprocessing.Pool() as read_pool:
            df_reader_list = read_pool.map(self.read_file, [(file, schemas, ds)])

        # Since read_csv_file returns a combined DataFrame, use the first element of df_reader_list
        combined_df = df_reader_list[0]

        # Split the DataFrame into chunks
        df_chunks = self.split_dataframe(combined_df, chunk_size=10000)

        # Use multiprocessing for writing the dataframes to Neo4j in parallel
        with multiprocessing.Pool() as write_pool:
            write_pool.map(self.write_chunk_neo4j, [(chunk, uri, db_conn['user'], db_conn['password'], ds, idx) for idx, chunk in enumerate(df_chunks)])

        driver.close()
        print(f'Finished processing file: {file}')   
