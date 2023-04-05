import config
import glob
import json
from utils.multiprocessor import MultiProcessor

def ingestion(ds_names=None):

    schemas = json.load(open(config.SCHEMAS_PATH))
    db_conn = {
            'uri': config.NEO4J_URI,
            'user': config.NEO4J_USER,
            'password': config.NEO4J_PASSWORD
        }

    if not ds_names:
        ds_names = schemas.keys()

    for ds in ds_names:
        files = glob.glob(f'{config.DATA_SRC_DIR}/{ds}/part-*')
        if len(files) == 0:
            raise NameError('No source files found for {ds} dataset')

        try:
            mp = MultiProcessor()
            for file in files:
                mp.process_file_neo4j(file, schemas, ds, db_conn)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'All data chunks loaded successfully to neo4j for {ds}\n')

if __name__ == '__main__':
    ingestion(['orders'])
