import pandas as pd
import re


class CsvReader():
    def __init__(self, file, schemas, ds_name):
        self.schemas = schemas
        self.file = file
        self.ds_name = ds_name
            
    def get_column_names(self, schemas, ds_name, sorting_key='column_position'):
        column_details = schemas[ds_name]
        columns = sorted(column_details, key=lambda col: col[sorting_key])
        return [col['column_name'] for col in columns]

    def read_csv_file(self):
        file_path_list = re.split('[/\\\]', self.file)
        ds_name = file_path_list[-2]
        file_name = file_path_list[-1]
        columns = self.get_column_names(self.schemas, self.ds_name)
        df_reader = pd.read_csv(self.file, names=columns, chunksize=10000)
        return list(df_reader)