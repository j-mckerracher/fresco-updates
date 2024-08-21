import io
import pandas as pd
import boto3
from tqdm.notebook import tqdm


class DatabaseManager():
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def list_s3_files(self, bucket_name, prefix):
        """
        List all files in an S3 bucket with a given prefix.

        :param bucket_name: The name of the S3 bucket.
        :param prefix: The prefix of the files to list.
        :return: A list of file keys.
        """
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [content['Key'] for content in response.get('Contents', [])]

    def load_parquet_files(self, bucket_name, prefix, start_time=None, end_time=None):
        """
        Load Parquet files from an S3 bucket into a pandas DataFrame, only loading files that match the date range.

        :param bucket_name: The name of the S3 bucket.
        :param prefix: The prefix of the files to load.
        :param start_time: The start time for filtering files (optional).
        :param end_time: The end time for filtering files (optional).
        :return: A pandas DataFrame containing the combined data from all relevant Parquet files.
        """
        file_keys = self.list_s3_files(bucket_name, prefix)
        chunks = []

        # Convert start_time and end_time to year-month format
        if start_time:
            start_year_month = start_time.strftime('%b%Y').lower()
        if end_time:
            end_year_month = end_time.strftime('%b%Y').lower()

        # Load each file into a pandas DataFrame if it falls within the date range
        for file_key in tqdm(file_keys, desc="Loading data"):
            # Extract the month and year from the file name
            file_date_part = file_key.split('_')[3]

            if start_time and end_time:
                # Load file if within the specified date range
                if start_year_month <= file_date_part <= end_year_month:
                    try:
                        obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
                        data = obj['Body'].read()  # Read the content of the S3 object into memory
                        df = pd.read_parquet(io.BytesIO(data))  # Wrap in BytesIO for seekable file-like object
                        chunks.append(df)
                    except Exception as e:
                        print(f"Error loading {file_key}: {e}")
                        raise
            else:
                # Load all files if no date range is specified
                obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
                data = obj['Body'].read()
                df = pd.read_parquet(io.BytesIO(data))
                chunks.append(df)

        # Combine all chunks into a single DataFrame
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

    def execute_query_on_dataframe(self, df, conditions, selected_columns, validate_time=False, start_time=None,
                                   end_time=None):
        """
        Executes filtering and querying on a pandas DataFrame.

        :param df: The pandas DataFrame to query.
        :param conditions: A list of conditions to filter the DataFrame.
        :param selected_columns: The columns to select from the DataFrame.
        :param validate_time: Boolean to determine whether to filter by a time range.
        :param start_time: The start time for filtering if validate_time is True.
        :param end_time: The end time for filtering if validate_time is True.
        :return: A filtered pandas DataFrame.
        """
        # Handle the case where '*' means select all columns
        if selected_columns == ('*',):
            query_df = df
        else:
            query_df = df[list(selected_columns)]

        local_conditions = []

        # Apply conditions
        for col, op, val in conditions:
            if op == "=":
                local_conditions.append(query_df[col] == val)
            elif op == "!=":
                local_conditions.append(query_df[col] != val)
            elif op == "<":
                local_conditions.append(query_df[col] < val)
            elif op == "<=":
                local_conditions.append(query_df[col] <= val)
            elif op == ">":
                local_conditions.append(query_df[col] > val)
            elif op == ">=":
                local_conditions.append(query_df[col] >= val)

        # Filter by time if required
        if validate_time:
            query_df['time'] = pd.to_datetime(query_df['time'])
            start_time = pd.to_datetime(start_time)
            end_time = pd.to_datetime(end_time)
            local_conditions.append((query_df['time'] >= start_time) & (query_df['time'] <= end_time))

        # Apply all conditions
        if local_conditions:
            combined_condition = local_conditions[0]
            for condition in local_conditions[1:]:
                combined_condition &= condition
            query_df = query_df[combined_condition]

        return query_df


