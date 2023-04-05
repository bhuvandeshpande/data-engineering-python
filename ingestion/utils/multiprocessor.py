from .reader import CsvReader
from .writer import Neo4JWriter
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
    
    def split_dataframe(self, df, chunk_size=10000):
    # Split the dataframe into smaller chunks
        return [df[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]

    def process_file_neo4j(self, file, schemas, ds, db_conn):
        print(f'Starting to process: {ds} {file} file')

        uri = db_conn['uri']
        driver = GraphDatabase.driver(uri, auth=(db_conn['user'], db_conn['password']))
        writer = Neo4JWriter()
        # Create and set the Neo4j database
        writer.create_and_set_neo4j_database(driver, ds, db_conn)

        # Use multiprocessing for reading the file in parallel
        with multiprocessing.Pool() as read_pool:
            df_list = read_pool.map(self.read_file, [(file, schemas, ds)])

        combined_df = df_list[0]

        # Split the DataFrame into chunks
        df_chunks = self.split_dataframe(combined_df, chunk_size=10000)

        # Use multiprocessing for writing the dataframes to Neo4j in parallel
        with multiprocessing.Pool() as write_pool:
            write_pool.map(writer.write_chunk_neo4j, [(chunk, db_conn, ds, idx) for idx, chunk in enumerate(df_chunks)])

        driver.close()
        print(f'Finished processing file: {file}')