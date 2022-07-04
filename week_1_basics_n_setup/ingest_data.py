#!/usr/bin/env python
# coding: utf-8

import argparse

import os
import pandas as pd
from sqlalchemy import create_engine
from pyarrow.parquet import ParquetFile
import pyarrow as pa
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    pq_f_name = 'output.parquet'

    os.system('wget -O {} {}'.format(pq_f_name, url))

    # engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, db))

    pf = ParquetFile(pq_f_name)

    pf_iter = pf.iter_batches(batch_size = 100000)

    df = pa.Table.from_batches([next(pf_iter)]).to_pandas()

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # this creates table schema
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # this inserts the data
    for pq_batch in pf_iter:
        t_start = time()

        df = pa.Table.from_batches([pq_batch]).to_pandas()

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took {:.3f} seconds'.format(t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data to Postgres')
    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the parquet
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='the table where will will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)