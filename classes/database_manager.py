import os
from typing import Optional
import psycopg2
from psycopg2 import OperationalError
import warnings
import pandas as pd
from tqdm.notebook import tqdm
import utilities.memory_utils as mem_utils


class DatabaseManager():
    def __init__(self):
        pass

    def get_database_connection(self) -> Optional[psycopg2.extensions.connection]:
        """
        Establish a connection to a PostgreSQL database using credentials stored in environment variables.

        This function retrieves database connection information from the environment variables
        'DBHOST', 'DBPW', 'DBNAME', and 'DBUSER', and attempts to establish a connection to the
        specified database. If any part of this process fails (e.g., a required environment
        variable is missing or the database connection cannot be established), an error message
        is printed and the function returns None.

        Returns:
        A psycopg2.extensions.connection object if the connection is successfully established;
        otherwise, None.
        """
        try:
            # Get the database credentials from the environment variables
            db_host = os.getenv('DBHOST')
            db_password = os.getenv('DBPW')
            db_name = os.getenv('DBNAME')
            db_user = os.getenv('DBUSER')

            if not all([db_host, db_password, db_name, db_user]):
                raise ValueError('One or more database credentials are missing from the environment variables.')

            # Establish a connection to the database
            connection = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_password)
            return connection

        except (Exception, OperationalError) as error:
            print(f"An error occurred: {error}")
            return None

    def execute_sql_query(self, query, incoming_df, params=None):
        """
        Executes the provided SQL query using the given database connection and parameters, and returns the result as a
        pandas DataFrame.

        Parameters:
        :param query: A string containing the SQL query to be executed.
        :param incoming_df: A pandas DataFrame in which the results of the SQL query will be stored.
        :param params: Optional. A list, tuple, or dict containing parameters to be passed to the SQL query. This is used
                       to handle parameterized queries safely.

        Returns:
        :return: A pandas DataFrame containing the results of the executed SQL query. If there's an error in execution or
                 establishing a database connection, the function may return None.
        """
        try:
            with self.get_database_connection() as conn:
                if conn is None:
                    print("Failed to establish a database connection.")
                    return

                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    incoming_df = pd.read_sql(query, conn, params=params)

                return incoming_df
        except Exception as e:
            print(f"An error occurred: {e}")

    def execute_sql_query_chunked(self, query, incoming_df, params=None, target_num_chunks=25000):
        """
        Executes the provided SQL query in chunks using the given database connection and parameters,
        and returns the combined result as a pandas DataFrame. This function is optimized for fetching
        large datasets by breaking the query into manageable chunks and processing them sequentially.

        Parameters:
        :param query: A string containing the SQL query to be executed.
        :param incoming_df: A pandas DataFrame which may be used to store intermediate results, though the final result
                            is appended to a new DataFrame.
        :param params: Optional. A list, tuple, or dict containing parameters to be passed to the SQL query.
                       This is used to handle parameterized queries safely.
        :param target_num_chunks: Optional. An integer that specifies the target number of chunks the dataset should
                                  be broken into. Default is 25000. The actual chunk size is determined by dividing
                                  the total number of rows by this value.

        Returns:
        :return: A pandas DataFrame containing the results of the executed SQL query combined from all chunks.
                 If there's an error in execution or establishing a database connection, the function may return None.
        """
        try:
            with self.get_database_connection() as conn:
                if conn is None:
                    print("Failed to establish a database connection.")
                    return

                # Create a cursor object
                with conn.cursor() as cur:
                    # Calculate total rows and chunk size
                    cur.execute(f"SELECT COUNT(*) FROM ({query}) as sub_query", params)
                    total_rows = cur.fetchone()[0]

                    chunksize = total_rows // target_num_chunks if total_rows > target_num_chunks else total_rows

                    # Fetch data with a progress bar
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        pbar = tqdm(total=total_rows,
                                    desc="Fetching rows",
                                    bar_format='{desc}: {percentage:.1f}%|{bar}| {n}/{total} [Elapsed: {elapsed} | '
                                               'Remaining: {remaining} | {rate_fmt}{postfix}]')

                        chunks = []
                        for chunk in pd.read_sql(query, conn, params=params, chunksize=chunksize):
                            chunks.append(chunk)
                            pbar.update(len(chunk))

                    pbar.close()
                    return pd.concat(chunks, ignore_index=True)
        except Exception as e:
            print(f"An error occurred: {e}")

    def execute_sql_query_and_stream_to_disk(self, query, output_directory, file_prefix, params=None,
                                             target_num_chunks=25000):
        """
        Executes the provided SQL query in chunks using the given database connection and parameters,
        and streams each chunk to a file on disk. This function is optimized for fetching large datasets
        by breaking the query into manageable chunks.

        Parameters:
        :param query: A string containing the SQL query to be executed.
        :param output_directory: Directory where chunk files will be saved.
        :param file_prefix: Prefix for each chunk file.
        :param params: Optional. Parameters to be passed to the SQL query.
        :param target_num_chunks: Optional. Target number of chunks for the dataset. Default is 25000.
        """
        try:
            with self.get_database_connection() as conn:
                if conn is None:
                    print("Failed to establish a database connection.")
                    return

                # make the output directory if it doesn't exist
                if not os.path.exists(output_directory):
                    os.makedirs(output_directory)

                with conn.cursor() as cur:
                    # Calculate total rows and chunk size
                    cur.execute(f"SELECT COUNT(*) FROM ({query}) as sub_query", params)
                    total_rows = cur.fetchone()[0]

                    chunksize = max(1, total_rows // target_num_chunks)

                    free_disk_space = mem_utils.get_disk_space()
                    total_space_used = 0

                    # Fetch data with a progress bar
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        pbar = tqdm(total=total_rows,
                                    desc="Fetching rows",
                                    bar_format='{desc}: {percentage:.1f}%|{bar}| {n}/{total} [Elapsed: {elapsed} | '
                                               'Remaining: {remaining} | {rate_fmt}{postfix}]')

                        for chunk_number, chunk in enumerate(pd.read_sql(query, conn, params=params, chunksize=chunksize)):
                            # Convert the chunk to a CSV string and get its size in bytes
                            csv_string = chunk.to_csv(index=False)
                            csv_size = len(csv_string.encode('utf-8'))

                            # Check if the size of the chunk plus the total space used so far is less than the available
                            # disk space
                            if total_space_used + csv_size < free_disk_space:
                                # Write the chunk to a CSV file
                                file_path = os.path.join(output_directory, f"{file_prefix}_{chunk_number}.csv")
                                chunk.to_csv(file_path, index=False)
                                pbar.update(len(chunk))

                                # Add the size of the chunk to the total space used
                                total_space_used += csv_size
                            else:
                                print(f"Not enough disk space to write chunk {chunk_number}. Ending process.")
                                return
                    pbar.close()

        except Exception as e:
            print(f"An error occurred: {e}")