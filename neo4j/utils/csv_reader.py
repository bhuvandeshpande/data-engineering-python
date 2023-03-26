import dask.dataframe as dd

class CSVReader:
    def __init__(self, file_path, chunksize):
        self.file_path = file_path
        self.chunksize = chunksize

    def read_csv_in_chunks(self):
        return dd.read_csv(self.file_path, blocksize=self.chunksize)
