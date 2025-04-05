import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    url = args.file_url
    table_name = args.table_name    
    file_name = 'output.csv' # can be parquet


    os.system(f"wget {url} -O {file_name}") # get file

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}') # sqlachemy to deal with conn to postgres


    # df = pd.read_parquet(f'{file_name}')
    df = pd.read_csv(f'{file_name}')

    # Generate the SQL schema (CREATE TABLE statement) for the DataFrame 'df'
    # 'name' specifies the name of the table in the database (e.g., 'yellow_taxi_data')
    # This schema can be used to create a table with the same structure as the DataFrame
    # print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    # create a db table from a dataframe
    print(f"Starting the write process for the file : {file_name} to the db")
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace'
    )
    print('Operation Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest csv data to postgres")

    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--db")
    parser.add_argument("--table_name")
    parser.add_argument("--file_url")
    args = parser.parse_args()

    main(args)

# To execute the script
# python3 ingest_data.py \
#     --user=root \
#     --password=root \
#     --host=localhost \
#     --port=5432 \
#     --db=ny_taxi \
#     --table_name=taxi_zone_lookup \
#     --file_url=https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# docker build -t taxi_ingest:v01 .

# docker run taxi_ingest:v01 \
#   --user=root \
#   --password=root \
#   --host=host.docker.internal \
#   --port=5432 \
#   --db=ny_taxi \
#   --table_name=taxi_zone_lookup \
#   --file_url=https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv













