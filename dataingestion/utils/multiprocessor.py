from .reader import CsvReader
from .dbwriter import DBWriter
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
