import config
import glob
import json
from utils.multiprocessor_new import MultiProcessor

def ingestion(ds_names=None, dest='psql'):

    schemas = json.load(open(config.SCHEMAS_PATH))

    if dest == 'psql':
        db_conn = f'postgresql://{config.PG_USER}:{config.PG_PASS}@{config.PG_HOST}:{config.PG_PORT}/{config.PG_DB}'
    else:
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
                if dest == 'psql':
                    mp.process_file(file, schemas, ds, db_conn)
                else:
                    mp.process_file_neo4j(file, schemas, ds, db_conn)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'All data chunks loaded successfully to {dest} for {ds}\n')

if __name__ == '__main__':
    ingestion(['orders'], dest='neo4j')
