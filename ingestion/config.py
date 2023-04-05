from dotenv import load_dotenv
import os

# Load the environment variables from the .env file
load_dotenv()

# Read the configuration values from the environment variables
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_SRC_DIR = os.path.join(BASE_DIR, *os.environ["DATA_SRC_DIR"].split('/'))
SCHEMAS_PATH = os.path.join(DATA_SRC_DIR, 'schemas.json')

# Read the configuration values from the environment variables
NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USER")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")