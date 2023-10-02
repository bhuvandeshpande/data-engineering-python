"""Entry point for the ETL application

Sample usage:
docker-compose run etl python main.py \
    --source /opt/data/activity.csv \
    --database warehouse
    --table user_activity
"""

# TODO: Implement a pipeline that loads the provided activity.csv file, performs the required
# transformations, and loads the result into the PostgreSQL table.

# Note: You can write the ETL flow with regular Python code, or you can also make use of a
# framework such as PySpark or others. The choice is yours.
import argparse
import csv
import psycopg2
from datetime import datetime, timedelta

def calculate_streak(dates):
    sorted_dates = sorted(dates)
    max_streak = 0
    current_streak = 1
    for i in range(1, len(sorted_dates)):
        if sorted_dates[i] - sorted_dates[i - 1] == timedelta(days=1):
            current_streak += 1
        else:
            max_streak = max(max_streak, current_streak)
            current_streak = 1
    return max(max_streak, current_streak)

def etl(source_file, database, table):
    conn = psycopg2.connect(
        dbname=database, 
        user="postgres", 
        password="postgres", 
        host="postgres"
    )
    cursor = conn.cursor()
    
    with open(source_file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        user_data = {}
        for row in reader:
            user_id = row['user_id']
            workspace_id = row['workspace_id']
            active_date = datetime.strptime(row['active_date'], "%Y-%m-%d").date()
            total_activity = int(row['total_activity'])
            
            if user_id not in user_data:
                user_data[user_id] = {'workspaces': {}, 'active_dates': set()}
            
            user_data[user_id]['active_dates'].add(active_date)
            user_data[user_id]['workspaces'].setdefault(workspace_id, 0)
            user_data[user_id]['workspaces'][workspace_id] += total_activity

        for user_id, data in user_data.items():
            longest_streak = calculate_streak(data['active_dates'])
            top_workspace = max(data['workspaces'], key=data['workspaces'].get)
            
            cursor.execute(
                f"INSERT INTO {table} (user_id, top_workspace, longest_streak) VALUES (%s, %s, %s)",
                (user_id, top_workspace, longest_streak)
            )
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ETL Pipeline')
    parser.add_argument('--source', required=True, help='Path to the source file')
    parser.add_argument('--database', required=True, help='Database name for inserting data')
    parser.add_argument('--table', required=True, help='Table name for inserting data')
    args = parser.parse_args()
    etl(args.source, args.database, args.table)
