from dotenv import load_dotenv
import os

# Load the environment variables from the .env file
load_dotenv()

# Read the configuration values from the environment variables
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_SRC_DIR = os.path.join(BASE_DIR, *os.environ["DATA_SRC_DIR"].split('/'))
SCHEMAS_PATH = os.path.join(DATA_SRC_DIR, 'schemas.json')
PG_HOST = os.environ["PG_HOST"]
PG_DB = os.environ["PG_DB"]
PG_PORT = os.environ["PG_PORT"]
PG_USER = os.environ["PG_USER"]
PG_PASS = os.environ["PG_PASS"]