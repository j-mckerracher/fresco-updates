import io
import os
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
import re
from scipy.stats import pearsonr
from matplotlib import pyplot as plt
import seaborn as sns
import zipfile
from typing import Optional
from psycopg2 import OperationalError
import warnings
from tqdm.notebook import tqdm


def remove_special_chars(s: str) -> str:
    """
    Removes any character that is not a letter, a number, or a comma from the input string.

    Parameters:
    :param s: A string from which special characters, excluding commas, will be removed.

    Returns:
    :return str: A string where all characters that are not letters, numbers, or commas have been removed.
    """
    # This pattern matches any character that is NOT a letter, number, or a comma
    pattern = r'[^a-zA-Z0-9,]'

    # Substitute all matches with an empty string
    cleaned_str = re.sub(pattern, '', s)

    return cleaned_str


def get_database_connection() -> Optional[psycopg2.extensions.connection]:
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


# Function to execute the SQL query using psycopg2
def execute_sql_query(query, incoming_df, params=None):
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
        with get_database_connection() as conn:
            if conn is None:
                print("Failed to establish a database connection.")
                return

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                incoming_df = pd.read_sql(query, conn, params=params)

            return incoming_df
    except Exception as e:
        print(f"An error occurred: {e}")


def execute_sql_query_chunked(query, incoming_df, params=None, target_num_chunks=25000):
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
        with get_database_connection() as conn:
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




def validate_jid(value):
    """
    Validates the provided job id (jid) value. The function checks if the value adheres to the predefined format.

    Parameters:
    :param value: A string containing the job id that needs to be validated.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the job id.
             Returns None if the value is valid.
    """

    jobs = value.split(',')
    for job in jobs:
        job = job.strip().upper()  # Remove any leading or trailing whitespace
        if not re.match(r'^JOB\d+$', job):
            return "Error: For 'jid', value must be a comma-separated list of strings starting with 'JOB' followed by one or more digits."
    return None


def validate_numeric_columns(column, value):
    """
    Validates the provided value for numeric columns. The function checks if the value can be converted to a float.

    Parameters:
    :param column: A string representing the column name for which the value needs to be validated. Possible values
                   include 'ncores', 'ngpus', 'nhosts', 'timelimit'.
    :param value: A string containing the value that needs to be validated based on the column criteria.

    Returns:
    :return: An error message string if the value cannot be converted to a float.
             Returns None if the value is valid.
    """
    try:
        float(value)  # Check if the value can be converted to a float (including integers and decimals)
    except ValueError:
        return f"Error: For '{column}', value must be a number (including decimals)."
    return None


def validate_account(value):
    """
    Validates the provided account value. The function checks if the value adheres to the predefined format.

    Parameters:
    :param value: A string containing the account that needs to be validated.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the account.
             Returns None if the value is valid.
    """
    groups = value.split(',')
    for group in groups:
        group = group.strip().upper()  # Remove any leading or trailing whitespace
        if not re.match(r'^GROUP\d+$', group):
            return "Error: For 'account', value must be a comma-separated list of strings starting with 'GROUP' followed by one or more digits."
    return None


def validate_username(value):
    """
    Validates the provided username value. The function checks if the value adheres to the predefined format.

    Parameters:
    :param value: A string containing the username that needs to be validated.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the username.
             Returns None if the value is valid.
    """

    users = value.split(',')
    for user in users:
        user = user.strip().upper()  # Remove any leading or trailing whitespace
        if not re.match(r'^USER\d+$', user):
            return "Error: For 'username', value must be a comma-separated list of strings starting with 'USER' followed by one or more digits."
    return None


def validate_host_list(value):
    """
    Validates the provided host list value. The function checks if the value adheres to the predefined format.

    Parameters:
    :param value: A string containing the host list that needs to be validated.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the host list.
             Returns None if the value is valid.
    """
    hosts = value.split(',')
    for host in hosts:
        host = host.strip().upper()  # Remove any leading or trailing whitespace
        if not re.match(r'^NODE\d+$', host):
            return "Error: For 'host_list', value must be a comma-separated list of strings starting with 'NODE' followed by one or more digits."
    return None


def validate_jobname(value):
    """
    Validates the provided jobname value. The function checks if the value adheres to the predefined format.

    Parameters:
    :param value: A string containing the jobname that needs to be validated.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the jobname.
             Returns None if the value is valid.
    """
    jobs = value.split(',')
    for job in jobs:
        job = job.strip().upper()  # Remove any leading or trailing whitespace
        if not re.match(r'^JOBNAME\d+$', job):
            return "Error: For job name, value must be a comma-separated list of strings starting with 'JOBNAME' followed by one or more digits."
    return None


def validate_condition_jobs(column, value):
    """
    Validates the provided value based on the specified column criteria. The function checks if the value for a
    particular column adheres to the predefined format.

    Parameters:
    :param column: A string representing the column name for which the value needs to be validated. Possible values
                   include 'jid', 'ncores', 'ngpus', 'nhosts', 'timelimit', 'account', 'username', 'host_list', and
                   'jobname'.
    :param value: A string containing the value that needs to be validated based on the column criteria.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the specified column.
             Returns None if the value is valid.
    """
    error_message = None
    if column == 'jid':
        error_message = validate_jid(value)
    elif column in ['ncores', 'ngpus', 'nhosts', 'timelimit']:
        error_message = validate_numeric_columns(column, value)
    elif column == 'account':
        error_message = validate_account(value)
    elif column == 'username':
        error_message = validate_username(value)
    elif column == 'host_list':
        error_message = validate_host_list(value)
    elif column == 'jobname':
        error_message = validate_jobname(value)
    return error_message


# Validate condition
def validate_condition_hosts(column, value):
    """
    Validates the provided value based on the specified column criteria for hosts. The function checks if the value
    for a particular column adheres to the predefined format.

    Parameters:
    :param column: A string representing the column name for which the value needs to be validated. Possible values
                   include 'event', 'host', 'jid', 'unit', and 'value'.
    :param value: A string or number containing the value that needs to be validated based on the column criteria.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the specified column.
             Returns None if the value is valid.
    """
    error_message = None
    if column == 'event':
        error_message = validate_event(value)
    elif column == 'host':
        error_message = validate_host(value)
    elif column == 'jid':
        error_message = validate_jid(value)
    elif column == 'unit':
        error_message = validate_unit(value)
    elif column == 'value':
        error_message = validate_value(value)
    return error_message


def validate_event(value):
    """
    Validates the provided event value. The function checks if the value is in the predefined list.

    Parameters:
    :param value: A string containing the event that needs to be validated.

    Returns:
    :return: An error message string if the value is not in the predefined list for the event.
             Returns None if the value is valid.
    """
    if value not in ['cpuuser', 'block', 'memused', 'memused_minus_diskcache', 'gpu_usage', 'nfs']:
        return "Error: For 'event', value must be one of: cpuuser, block, memused, memused_minus_diskcache, gpu_usage, nfs."
    return None


def validate_host(value):
    """
    Validates the provided host value. The function checks if the value adheres to the predefined format.

    Parameters:
    :param value: A string containing the host that needs to be validated.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the host.
             Returns None if the value is valid.
    """
    hosts = value.split(',')
    for host in hosts:
        host = host.strip().upper()  # Remove any leading or trailing whitespace
        if not re.match(r'^NODE\d+$', host):
            return "Error: For 'host', value must be a comma-separated list of strings starting with 'NODE' followed by one or more digits."
    return None


def validate_unit(value):
    """
    Validates the provided unit value. The function checks if the value is in the predefined list.

    Parameters:
    :param value: A string containing the unit that needs to be validated.

    Returns:
    :return: An error message string if the value is not in the predefined list for the unit.
             Returns None if the value is valid.
    """
    if value not in ['CPU %', 'GPU %', 'GB:memused', 'GB:memused_minus_diskcache', 'GB/s', 'MB/s']:
        return "Error: For 'unit', value must be one of: 'CPU %', 'GPU %', 'GB:memused', 'GB:memused_minus_diskcache', 'GB/s', 'MB/s'."
    return None


def validate_value(value):
    """
    Validates the provided value for numeric columns. The function checks if the value can be converted to a float.

    Parameters:
    :param value: A string containing the value that needs to be validated.

    Returns:
    :return: An error message string if the value cannot be converted to a float.
             Returns None if the value is valid.
    """
    try:
        float(value)
    except ValueError:
        return "Error: For 'value', the value must be a number."
    return None


def validate_jid(value):
    """
    Validates the provided job id (jid) value. The function checks if the value adheres to the predefined format.

    Parameters:
    :param value: A string containing the job id that needs to be validated.

    Returns:
    :return: An error message string if the value does not adhere to the predefined format for the job id.
             Returns None if the value is valid.
    """
    jobs = value.split(',')
    for job in jobs:
        job = job.strip().upper()  # Remove any leading or trailing whitespace
        if not re.match(r'^JOB\d+$', job):
            return "Error: For 'jid', value must be a comma-separated list of strings starting with 'JOB' followed by one or more digits."
    return None


# Construct SQL query
def construct_query_hosts(where_conditions_hosts, host_data_columns_dropdown, validate_button_hosts, start_time_hosts,
                          end_time_hosts):
    """
    Constructs an SQL query based on the specified conditions and selected columns for host data retrieval.

    Parameters:
    :param where_conditions_hosts: A list of tuples, where each tuple contains three elements - the column name, the
                                   operation (e.g., '=', '<>', '<', '>'), and the value to be used in the WHERE clause.
    :param host_data_columns_dropdown: A list of strings, each representing a selected column for the query.
    :param validate_button_hosts: A string indicating the validation status for time. If set to "Times Valid", the
                                  start_time_hosts and end_time_hosts are considered.
    :param start_time_hosts: A datetime object or string representing the start time for the query's time condition.
                             This is considered if validate_button_hosts is set to "Times Valid".
    :param end_time_hosts: A datetime object or string representing the end time for the query's time condition. This
                           is considered if validate_button_hosts is set to "Times Valid".

    Returns:
    :return: A tuple containing two elements. The first element is a string representing the constructed SQL query. The
             second element is a list containing the parameter values to be used in the query.
    """
    selected_columns = ', '.join(host_data_columns_dropdown)
    table_name = 'host_data'
    query = f"SELECT {selected_columns} FROM {table_name}"

    params = []
    where_clause = ""
    if where_conditions_hosts:
        where_clause = " AND ".join([f"{col} {op} %s" for col, op, _ in where_conditions_hosts])
        params = [val for _, _, val in where_conditions_hosts]
        query += f" WHERE {where_clause}"

    if validate_button_hosts == "Times Valid":
        if where_conditions_hosts:
            query += f" AND time BETWEEN %s AND %s"
        else:
            query += f" WHERE time BETWEEN %s AND %s"
        params += [start_time_hosts, end_time_hosts]

    return query, params


# Construct SQL query for job_data
def construct_job_data_query(where_conditions_jobs, job_data_columns_dropdown, validate_button_jobs, start_time_jobs,
                             end_time_jobs):
    """
    Constructs an SQL query based on the specified conditions and selected columns for job data retrieval.

    Parameters:
    :param where_conditions_jobs: A list of tuples, where each tuple contains three elements - the column name, the
                                  operation (e.g., '=', '<>', '<', '>'), and the value to be used in the WHERE clause.
    :param job_data_columns_dropdown: A list of strings, each representing a selected column for the query.
    :param validate_button_jobs: A string indicating the validation status for time. If set to "Times Valid", the
                                 start_time_jobs and end_time_jobs are considered.
    :param start_time_jobs: A datetime object or string representing the start time for the query's time condition.
                            This is considered if validate_button_jobs is set to "Times Valid".
    :param end_time_jobs: A datetime object or string representing the end time for the query's time condition. This
                          is considered if validate_button_jobs is set to "Times Valid".

    Returns:
    :return: A tuple containing two elements. The first element is a string representing the constructed SQL query. The
             second element is a list containing the parameter values to be used in the query.
    """
    valid_columns = {'jid', 'submit_time', 'start_time', 'end_time', 'runtime', 'timelimit', 'node_hrs', 'nhosts',
                     'ncores', 'ngpus', 'username', 'account', 'queue', 'state', 'jobname', 'exitcode', 'host_list',
                     '*'}
    selected_columns = job_data_columns_dropdown

    # Validate that the selected columns are all in the set of valid columns
    if not all(column in valid_columns for column in selected_columns):
        raise ValueError("Invalid column name selected")

    selected_columns_str = ', '.join(selected_columns)
    table_name = 'job_data'
    query = f"SELECT {selected_columns_str} FROM {table_name}"

    params = []
    where_clause = ""
    if where_conditions_jobs:
        where_clause = " AND ".join([f"{col} {op} %s" for col, op, _ in where_conditions_jobs])
        params = [val for _, _, val in where_conditions_jobs]
        query += f" WHERE {where_clause}"

    if validate_button_jobs == "Times Valid":
        if where_conditions_jobs:
            query += f" AND start_time BETWEEN %s AND %s"
        else:
            query += f" WHERE start_time BETWEEN %s AND %s"
        params += [start_time_jobs, end_time_jobs]

    return query, params


def get_mean(time_series: pd.DataFrame, rolling=False, window=None) -> pd.DataFrame:
    """
    Calculates the mean of the provided time_series DataFrame, either on the entire DataFrame or on a rolling window
    basis.

    Parameters:
    :param time_series: A pandas DataFrame that contains a column 'value'. The median is calculated on the 'value'
                        column.
    :param rolling: A boolean indicating whether the median should be calculated for the entire DataFrame (False) or
                    on a rolling window basis (True).
    :param window: The size of the rolling window for which the median is to be calculated. It should be specified
                   in string format like use D for days, H for hours, T for minutes, and S for seconds. This parameter
                   is considered only if rolling is set to True.

    Returns:
    :return: A pandas DataFrame or Series which contains the median of the 'value' column of the provided time_series
             DataFrame. If rolling is set to True, the result will be a DataFrame with the rolling window median. If
             rolling is False, the result will be a Series with the overall median.
    """
    result = time_series.copy()
    result.drop(['jid', 'host', 'event', 'unit'], axis=1, inplace=True)

    if rolling:
        return result.rolling(window=window).mean()

    return result.mean()


def get_median(time_series: pd.DataFrame, rolling=False, window=None) -> pd.DataFrame:
    """
    Calculates the median of the provided time_series DataFrame, either on the entire DataFrame or on a rolling window
    basis.

    Parameters:
    :param time_series: A pandas DataFrame that contains a column 'value'. The median is calculated on the 'value'
                        column.
    :param rolling: A boolean indicating whether the median should be calculated for the entire DataFrame (False) or
                    on a rolling window basis (True).
    :param window: The size of the rolling window for which the median is to be calculated. It should be specified
                   in string format like use D for days, H for hours, T for minutes, and S for seconds. This parameter
                   is considered only if rolling is set to True.

    Returns:
    :return: A pandas DataFrame or Series which contains the median of the 'value' column of the provided time_series
             DataFrame. If rolling is set to True, the result will be a DataFrame with the rolling window median. If
             rolling is False, the result will be a Series with the overall median.
    """
    result = time_series.copy()
    result.drop(['jid', 'host', 'event', 'unit'], axis=1, inplace=True)

    if rolling:
        return result.rolling(window=window).median()

    return result.median()


def get_standard_deviation(time_series: pd.DataFrame, rolling=False, window=None) -> pd.DataFrame:
    """
    Calculates the standard deviation of the provided time_series DataFrame, either on the entire DataFrame or on a
    rolling window basis.

    Parameters:
    :param time_series: A pandas DataFrame that contains a column 'value'. The standard deviation is calculated on the
                        'value' column.
    :param rolling: A boolean indicating whether the standard deviation should be calculated for the entire DataFrame
                    (False) or on a rolling window basis (True).
    :param window: The size of the rolling window for which the standard deviation is to be calculated. It should be
                    specified in string format like use D for days, H for hours, T for minutes, and S for seconds. This
                    parameter is considered only if rolling is set to True.

    Returns:
    :return: A pandas DataFrame or Series which contains the standard deviation of the 'value' column of the provided
            time_series DataFrame. If rolling is set to True, the result will be a DataFrame with the rolling window
            standard deviation. If rolling is False, the result will be a Series with the overall standard deviation.
    """
    result = time_series.copy()
    result.drop(['jid', 'host', 'event', 'unit'], axis=1, inplace=True)

    if rolling:
        return result.rolling(window=window).std()

    return result.std()


def plot_pdf(ts_df: pd.DataFrame):
    """
    This function generates a Probability Density Function (PDF) plot for a given time series DataFrame.

    Parameters:
    :param ts_df: A pandas DataFrame that contains a column 'value'. This column should represent the time series
                  data for which the PDF will be calculated and plotted.

    Returns:
    :return: This function does not return anything. Instead, it creates a histogram and overlaid kernel density
             estimate plot using seaborn's histplot function. The x-axis of the plot represents the 'value' column
             from the input DataFrame, and the y-axis represents the estimated probability density. The title of
             the plot is 'Probability Density Function (PDF)'.
    """
    time_series_df = ts_df.dropna()
    sns.histplot(time_series_df['value'], kde=True)
    plt.title('Probability Density Function (PDF)')
    plt.show()


def plot_cdf(ts_df: pd.DataFrame):
    """
    This function generates a Cumulative Distribution Function (CDF) plot for a given time series DataFrame.

    Parameters:
    :param ts_df: A pandas DataFrame that contains a column 'value'. This column should represent the time series
                  data for which the CDF will be calculated and plotted.

    Returns:
    :return: This function does not return anything. Instead, it creates a step plot using seaborn's histplot function,
             with the cumulative option set to True. The x-axis of the plot represents the 'value' column from the
             input DataFrame, and the y-axis represents the cumulative frequency. The title of the plot is
             'Cumulative Distribution Function (CDF)'.
    """
    time_series_df = ts_df.dropna()
    sns.histplot(time_series_df['value'], cumulative=True, element="step", fill=False)
    plt.title('Cumulative Distribution Function (CDF)')
    plt.show()


def plot_data_points_outside_threshold(ratio_threshold_value, ts_df: pd.DataFrame):
    """
    Plots the ratio of data points that lie outside a specified threshold from a given time series DataFrame. The
    function displays a bar chart indicating the proportions of data points inside and outside the threshold.

    Parameters:
    :param ratio_threshold_value: A float or integer representing the threshold value against which data points in the
                                  'value' column of the time series DataFrame will be compared.
    :param ts_df: A pandas DataFrame containing time series data. The DataFrame should have a column named 'value'
                  which contains the data points to be evaluated.

    Returns:
    This function does not return any value. Instead, it displays a bar chart.
    """
    threshold = ratio_threshold_value

    # Here we calculate the ratio of data outside the threshold
    num_data_points = len(ts_df['value'])
    num_outside_threshold = sum(abs(ts_df['value']) > threshold)

    ratio_outside_threshold = num_outside_threshold / num_data_points

    # Plotting
    plt.bar(['Inside Threshold', 'Outside Threshold'], [1 - ratio_outside_threshold, ratio_outside_threshold])
    plt.title('Ratio of Data Outside Threshold')
    plt.ylabel('Ratio')
    plt.show()


def parse_host_data_query(sql_query: tuple, mapped_units: dict):
    """
    Parses the provided SQL query tuple to identify matched keywords based on the provided mapped units dictionary.
    The function attempts to find matches between the query parameters and the keys or values of the mapped_units
    dictionary.

    Parameters:
    :param sql_query: A tuple containing the SQL query string and its associated parameters.
    :param mapped_units: A dictionary where keys represent unit names and values represent corresponding mapped
                         representations of those units.

    Returns:
    :return: A list of strings containing the matched keywords from the mapped_units dictionary based on the SQL
             query's parameters. If no matches are found or an error occurs, it returns all the keys from the
             mapped_units dictionary.
    """
    if not isinstance(sql_query, tuple) or len(sql_query) < 2:
        return list(mapped_units.keys())

    _, params = sql_query

    matched_keywords = []

    try:
        # Check the unit value in params (params[0])
        for key, value in mapped_units.items():
            if params[0] == key or params[0] == value:
                matched_keywords.append(key)

        # If no matches are found, return all the keys from the mapped_units
        if not matched_keywords:
            return list(mapped_units.keys())

        return matched_keywords
    except Exception as e:
        print(f"An error occurred: {e}")
        return list(mapped_units.keys())


def plot_box_and_whisker(df_mean: pd.DataFrame, df_std: pd.DataFrame, df_median: pd.DataFrame):
    """
    This function generates a box and whisker plot for various statistics (mean, median, and standard deviation)
    of a time series data, as contained within provided DataFrames.

    Parameters:
    :param df_mean: A pandas DataFrame that contains a column 'value' with the mean values of the time series data.
    :param df_std: A pandas DataFrame that contains a column 'value' with the standard deviation values of the time series data.
    :param df_median: A pandas DataFrame that contains a column 'value' with the median values of the time series data.

    Returns:
    :return: This function does not return anything. Instead, it plots a box and whisker plot for the provided
             statistics. Each box in the plot represents one of the statistics. The box extends from the lower to
             upper quartile values of the data, with a line at the median. The whiskers extend from the box to show
             the range of the data. Outlier points are those past the end of the whiskers.
    """
    # Collect statistics into a list of pandas Series or numpy arrays
    all_data = []
    labels = []
    color_choices = []
    if df_mean is not None:
        df_mean.dropna(subset=['value'], inplace=True)
        all_data.append(df_mean['value'])
        labels.append('Mean')
        color_choices.append('pink')
    if df_median is not None:
        df_median.dropna(subset=['value'], inplace=True)
        all_data.append(df_median['value'])
        labels.append('Median')
        color_choices.append('lightgreen')
    if df_std is not None:
        df_std.dropna(subset=['value'], inplace=True)
        all_data.append(df_std['value'])
        labels.append('Standard Deviation')
        color_choices.append('lightyellow')

    # Create a new figure and axis for the box plot
    fig, ax = plt.subplots()

    # Create the box plot
    if len(all_data) > 0:
        bplot = ax.boxplot(all_data,
                           vert=True,  # vertical box alignment
                           notch=True,  # notch shape
                           patch_artist=True,  # fill with color
                           labels=labels)  # will be used to label x-ticks

        # fill with colors
        colors = color_choices
        for patch, color in zip(bplot['boxes'], colors):
            patch.set_facecolor(color)

        # adding horizontal grid lines
        ax.yaxis.grid(True)
        ax.set_xlabel('Statistic')
        ax.set_ylabel('Values')

        plt.show()


def calculate_and_plot_correlation(time_series: pd.DataFrame, correlations):
    """
    This function calculates the Pearson Correlation Coefficient between two time series.

    Parameters:
    time_series: A pandas DataFrame that contains a column 'value'. This column should represent the time series
                 data for which the Pearson Correlation Coefficient will be calculated.
    correlations: A tuple of two elements, each representing the metric to be used for correlation calculation.

    Returns:
    This function does not return anything. Instead, it prints the Pearson Correlation Coefficient
    between the two time series.
    """
    metric_one, metric_two = correlations

    ts_metric_one = time_series[time_series['event'] == metric_one]
    ts_metric_one = ts_metric_one[~ts_metric_one.index.duplicated(keep='first')]

    ts_metric_two = time_series[time_series['event'] == metric_two]
    ts_metric_two = ts_metric_two[~ts_metric_two.index.duplicated(keep='first')]

    insufficient_data = []
    if len(ts_metric_one) < 2:
        insufficient_data.append(metric_one)
    if len(ts_metric_two) < 2:
        insufficient_data.append(metric_two)

    if insufficient_data:
        print(f"Insufficient data for {', '.join(insufficient_data)}")
        return

    # find common timestamps using index intersection
    common_timestamps = ts_metric_one.index.intersection(ts_metric_two.index)

    metric_one_values = ts_metric_one.loc[common_timestamps, 'value'].values
    metric_two_values = ts_metric_two.loc[common_timestamps, 'value'].values

    # Check if both lists have the same length and at least 2 data points
    if len(metric_one_values) != len(metric_two_values):
        print(f'The two metrics do not have the same amount of sampling in the data, '
              f'one is {len(metric_one_values)}, and the other is {len(metric_two_values)}')
        return
    elif len(metric_one_values) < 2 or len(metric_two_values) < 2:
        print('Both time series need to have at least 2 data points to calculate correlation.')
        return
    else:
        correlation, p_val = pearsonr(metric_one_values, metric_two_values)

        # Create a scatter plot
        plt.figure(figsize=(10, 6))
        plt.scatter(metric_one_values, metric_two_values)
        plt.xlabel(correlations[0])
        plt.ylabel(correlations[1])
        plt.title(f'Scatter plot of {metric_one} and {metric_two}')

        # Add a text box with the correlation and p-value
        text_str = f'Correlation: {correlation:.10f}\nP-value: {p_val:.10f}'
        plt.text(0.05, 0.95, text_str, transform=plt.gca().transAxes, fontsize=14,
                 verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        plt.show()

        return {"Correlation": correlation, "P-value": p_val}


def create_csv_download_file(df, filename="data.csv"):
    """
    Saves a DataFrame as a zipped CSV file to the current working directory.

    Parameters:
    :param df: A pandas DataFrame that is to be saved.
    :param filename: The name to use for the saved file inside the zip. Defaults to "data.csv".

    Returns:
    :return: A message indicating the file's location or an error message.
    """
    try:
        # Convert DataFrame to CSV string
        csv_str = df.to_csv(index=False)
        csv_bytes = csv_str.encode('utf-8')

        # Define zip filename
        zip_filename = filename.rsplit('.', 1)[0] + '.zip'
        zip_path = os.path.join(os.getcwd(), zip_filename)

        # Write the CSV data to a zip file
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(filename, csv_bytes)

        return f"File saved to {zip_path}"
    except Exception as e:
        return f"An error occurred: {e}"


def create_excel_download_file(df, filename="data.xlsx"):
    """
    Saves a DataFrame as a zipped Excel (.xlsx) file to the current working directory.

    Parameters:
    :param df: A pandas DataFrame that is to be saved.
    :param filename: The name to use for the saved file inside the zip. Defaults to "data.xlsx".

    Returns:
    :return: A message indicating the file's location or an error message.
    """
    try:
        df_copy = df.copy()

        # If df has datetime columns, convert them to timezone-naive
        for col in df.columns:
            if isinstance(df[col].dtype, pd.DatetimeTZDtype):
                df_copy[col] = df[col].dt.tz_convert(None)

        # Write DataFrame to Excel in memory
        with io.BytesIO() as output:
            df_copy.to_excel(output, engine='xlsxwriter', sheet_name='Sheet1')
            excel_data = output.getvalue()

        # Define zip filename
        zip_filename = filename.rsplit('.', 1)[0] + '.zip'
        zip_path = os.path.join(os.getcwd(), zip_filename)

        # Write the Excel data to a zip file
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(filename, excel_data)

        return f"File saved to {zip_path}"
    except Exception as e:
        return f"An error occurred: {e}"


def remove_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
       Removes specific columns ('type', 'diff', 'arc') from the given dataframe.

       Parameters:
       :param df: A pandas DataFrame

       Returns:
       :return: A pandas DataFrame with the specified columns removed.
       """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame.")

    columns_to_remove = ['type', 'diff', 'arc']

    try:
        return df.drop(columns=columns_to_remove, errors='ignore')
    except Exception as e:
        raise RuntimeError(f"An error occurred while removing columns: {e}")
