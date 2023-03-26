import dask.dataframe as dd

class CSVReader:
    def __init__(self, file_path, chunksize):
        self.file_path = file_path
        self.chunksize = chunksize

    def read_csv_in_chunks(self):
        try:
            return dd.read_csv(self.file_path, blocksize=self.chunksize)
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            raise
