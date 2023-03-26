import config
from utils.csv_reader import CSVReader
from utils.neo4j_writer import Neo4jWriter
from dask.distributed import Client
from dask.delayed import delayed

def main():
    # Initialize CSVReader and Neo4jWriter
    csv_reader = CSVReader(config.CSV_FILE_PATH, config.CHUNK_SIZE)
    neo4j_writer = Neo4jWriter(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASSWORD)

    # Read the CSV file in chunks using Dask
    ddf = csv_reader.read_csv_in_chunks()

    # Start a Dask distributed client
    client = Client()

    # Use Dask to parallelize the processing of the CSV file
    lazy_results = []
    for chunk in ddf.to_delayed():
        result = delayed(neo4j_writer.process_chunk)(chunk.compute())
        lazy_results.append(result)

    # Compute the results and wait for completion
    results = client.compute(lazy_results, sync=True)
    total_rows_processed = sum(results)
    print(f"Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
