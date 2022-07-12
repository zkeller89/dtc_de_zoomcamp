import os
import pandas as pd
from sqlalchemy import create_engine
from pyarrow.parquet import ParquetFile
import pyarrow as pa
from time import time

def ingest_callable(user, password, host, port, db, table_name, pq_f_name):
    print(table_name, pq_f_name)

    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, db))
    engine.connect()

    print('connection established successfully, inserting data...')

    pf = ParquetFile(pq_f_name)

    pf_iter = pf.iter_batches(batch_size = 100000)

    df = pa.Table.from_batches([next(pf_iter)]).to_pandas()

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # this creates table schema
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    t_start = time()
    df.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()
    print('inserted first chunk..., took {:.3f} seconds'.format(t_end - t_start))

    # this inserts the data
    for pq_batch in pf_iter:
        t_start = time()

        df = pa.Table.from_batches([pq_batch]).to_pandas()

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk..., took {:.3f} seconds'.format(t_end - t_start))