import os
from datetime import datetime
from typing import Optional
from pyathena import connect
import warnings
import pandas as pd
from tqdm.notebook import tqdm


class DatabaseManager():
    def __init__(self, s3_staging_dir: str = 's3://fresco-database-athena-results/', region_name: str = 'us-east-1'):
        self.s3_staging_dir = s3_staging_dir
        self.region_name = region_name

    def get_database_connection(self):
        try:
            # Establish a connection to Athena
            connection = connect(
                s3_staging_dir=self.s3_staging_dir,
                region_name=self.region_name
            )
            return connection
        except Exception as error:
            print(f"An error occurred: {error}")
            return None

    def execute_sql_query(self, query: str, params: Optional[dict] = None) -> Optional[pd.DataFrame]:
        """
        Executes the provided SQL query and returns the result as a pandas DataFrame.
        """
        try:
            with self.get_database_connection() as conn:
                if conn is None:
                    print("Failed to establish a database connection.")
                    return None

                # Manually format the query with the actual parameter values
                if params:
                    for key, value in params.items():
                        if isinstance(value, datetime):
                            value = value.strftime('%Y-%m-%d %H:%M:%S')
                        query = query.replace(f":{key}", f"CAST('{value}' AS timestamp)")

                # Prepend the database name to the table in the query
                query = query.replace('FROM host_data', 'FROM fresco_data.host_data')
                query = query.replace('FROM job_data', 'FROM fresco_data.job_data')

                print(f"Formatted query: {query}")

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    df = pd.read_sql(query, conn)

                return df
        except Exception as e:
            print(f"An error occurred while executing the query: {e}")
            return None

    def execute_sql_query_chunked(self, query, incoming_df, params=None, target_num_chunks=25000) -> Optional[
        pd.DataFrame]:
        """
        Executes the provided SQL query in chunks, optimized for fetching large datasets.
        """
        try:
            with self.get_database_connection() as conn:
                if conn is None:
                    print("Failed to establish a database connection.")
                    return None

                # Manually format the query with the actual parameter values
                if params:
                    for key, value in params.items():
                        if isinstance(value, datetime):
                            value = value.strftime('%Y-%m-%d %H:%M:%S')
                        query = query.replace(f":{key}", f"CAST('{value}' AS timestamp)")

                # Prepend the database name to the table in the query
                query = query.replace('FROM host_data', 'FROM fresco_data.host_data')
                query = query.replace('FROM job_data', 'FROM fresco_data.job_data')

                print(f"Formatted query: {query}")

                # Calculate total rows and chunk size
                total_rows = pd.read_sql(f"SELECT COUNT(*) FROM ({query}) as sub_query", conn).iloc[0, 0]
                chunksize = total_rows // target_num_chunks if total_rows > target_num_chunks else total_rows

                # Fetch data with a progress bar
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    pbar = tqdm(total=total_rows,
                                desc="Fetching rows",
                                bar_format='{desc}: {percentage:.1f}%|{bar}| {n}/{total} [Elapsed: {elapsed} | '
                                           'Remaining: {remaining} | {rate_fmt}{postfix}]')

                    chunks = []
                    for chunk in pd.read_sql(query, conn, chunksize=chunksize):
                        chunks.append(chunk)
                        pbar.update(len(chunk))

                pbar.close()
                return pd.concat(chunks, ignore_index=True)
        except Exception as e:
            print(f"An error occurred while executing the chunked query: {e}")
            return None
