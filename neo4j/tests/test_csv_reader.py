import os
import tempfile
import pandas as pd
from utils.csv_reader import CSVReader

def test_read_csv_in_chunks():
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp_file:
        temp_file.write("source,target,relationship_type\n")
        temp_file.write("1,2,connected\n")
        temp_file.write("2,3,connected\n")
        temp_file.write("3,4,connected\n")

    csv_reader = CSVReader(temp_file.name, chunksize=2)
    ddf = csv_reader.read_csv_in_chunks()

    assert len(ddf) == 3

    # Clean up the temporary file
    os.remove(temp_file.name)
    