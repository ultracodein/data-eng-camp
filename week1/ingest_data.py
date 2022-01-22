#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    trip_table_name = params.trip_table_name
    zone_table_name = params.zone_table_name

    trip_csv_name = 'yellow_tripdata_2021-01.csv'
    zone_csv_name = 'taxi+_zone_lookup.csv'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(trip_csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=trip_table_name, con=engine, if_exists='replace')
    df.to_sql(name=trip_table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=trip_table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

    df2 = pd.read_csv(zone_csv_name)
    df2.to_sql(name=zone_table_name, con=engine, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--trip_table_name', help='name of the table where we will write the trip info')
    parser.add_argument('--zone_table_name', help='name of the table where we will write the zone info')

    args = parser.parse_args()
    main(args)