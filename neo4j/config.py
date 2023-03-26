from dotenv import load_dotenv
import os

# Load the environment variables from the .env file
load_dotenv()

# Read the configuration values from the environment variables
NEO4J_URI = os.environ["NEO4J_URI"]
NEO4J_USER = os.environ["NEO4J_USER"]
NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]
CSV_FILE_PATH = os.environ["CSV_FILE_PATH"]
CHUNK_SIZE = int(os.environ["CHUNK_SIZE"])
