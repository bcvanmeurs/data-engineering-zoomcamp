import argparse
import os
from time import time

from pyarrow.parquet import ParquetFile
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    filename = url.split("/")[-1]
    if not os.path.exists(filename):
        os.system(f"wget {url} -O {filename}")

    # data = pd.read_parquet(filename)
    # print(data.head())
    # print(data.dtypes)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    # df_iter = pd.read_parquet(filename, iterator=True, chunksize=100000)
    # df = next(df_iter)

    print("Read Parquet")
    pf = ParquetFile(filename)
    print("Create Iterator")
    df_iter = iter(pf.iter_batches(batch_size=100000))
    print("Load df")
    df = next(df_iter).to_pandas()
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        try:
            t_start = time()
            df = next(df_iter).to_pandas()
            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()

            print("inserted another chunk, took %.3f second" % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the csv file")

    args = parser.parse_args()

    main(args)
