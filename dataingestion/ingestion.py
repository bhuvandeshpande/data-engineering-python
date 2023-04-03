from csvreader.reader import CsvReader
from postgreswriter.dbwriter import DBWriter
import config
import glob
import os
import json

def ingestion(ds_names=None):

    schemas = json.load(open(config.SCHEMAS_PATH))
    db_conn = f'postgresql://{config.PG_USER}:{config.PG_PASS}@{config.PG_HOST}:{config.PG_PORT}/{config.PG_DB}'

    # If no ds_names are passed then the ingestion.py will process all csv files mentioned in the schemas.json else it will run for specified ds_names
    if not ds_names:
        ds_names = schemas.keys()
    
    for ds in ds_names:
        files = glob.glob(f'{config.DATA_SRC_DIR}/{ds}/part-*')
        if len(files) == 0:
            raise NameError('No source files found for {ds} dataset')
        
        try:
            for file in files:
                # Instantiate csv reader object to read csv files
                cv = CsvReader(file, schemas, ds)
                df_reader = cv.read_csv_file()            
                for idx, df in enumerate(df_reader):
                    print(f'Processing chunk {idx} with size {df.shape[0]} of {ds} table')
                    dbwriter = DBWriter(df, db_conn, ds)
                    dbwriter.write_to_postgres()
                    print(f'Data chunk {idx} with size {df.shape[0]} loaded successfully to table retail.{ds}')
                print(f'Check data loaded to table retail.{ds}......')
                query = f'SELECT * FROM retail.{ds} LIMIT 10'
                print(query)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'All data chunks loaded successfully to table retail.{ds}')


    # Instantiate postgres writer object to write data to postgres database
    

if __name__ == '__main__':
    ingestion()
