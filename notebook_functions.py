import base64
import io
import os
import ipywidgets
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from IPython.display import display, FileLink, HTML
from ipywidgets import widgets
from datetime import datetime
import re
from scipy.stats import pearsonr
from matplotlib import pyplot as plt
import seaborn as sns


# ---------- UTILITY FUNCTIONS ------------

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


def extract_month_year(date_string: str) -> tuple:
    """
    This function extracts the month (in 3 letter form, e.g. 'Jan') and the year
    (in four digit form, e.g. '2022') from a string in the format 'mm-dd-yyyy hh:mm:ss'.

    :param date_string: A string representing a date in the format 'mm-dd-yyyy hh:mm:ss'.
    :return: Two strings representing the month and the year extracted from the input date string.
    """
    date = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    month = date.strftime('%b')  # returns month in 3 letter form, e.g. 'Jan'
    year = date.strftime('%Y')  # returns year in 4 digits, e.g. '2022'
    return month, year


# -------------- CELL 3 --------------
def get_time_series_from_database(start_time, end_time) -> pd.DataFrame:
    """
    This function should read the data from the database.

    :param start_time: The start time in the format of '%Y-%m-%d %H:%M:%S'.
    :param end_time: The end time in the format of '%Y-%m-%d %H:%M:%S'.
    :return: A pandas DataFrame containing the data.
    """
    # Get the database credentials from the environment variables
    db_host = os.getenv('DBHOST')
    db_password = os.getenv('DBPW')
    db_name = os.getenv('DBNAME')
    db_user = os.getenv('DBUSER')

    # Establish a connection to the database
    # conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_password)
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres", password="Envision9")

    # Create a cursor object
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Define the SQL query
    sql_query = """
            SELECT *
            FROM public.host_data
            WHERE time >= %s AND time <= %s
        """

    # Execute the SQL query
    cur.execute(sql_query, (start_time, end_time))

    # Fetch all rows from the last executed SQL query
    rows = cur.fetchall()

    # Convert the results into a pandas DataFrame
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    # Close the cursor and connection
    cur.close()
    conn.close()

    return df


def get_account_log_from_database(start_time, end_time) -> pd.DataFrame:
    """
    This function should get the account data from the database.

    :param start_time: The start time in the format of '%Y-%m-%d %H:%M:%S'.
    :param end_time: The end time in the format of '%Y-%m-%d %H:%M:%S'.
    :return: A pandas DataFrame containing the data.
    """
    # Get the database credentials from the environment variables
    db_host = os.getenv('DBHOST')
    db_password = os.getenv('DBPW')
    db_name = os.getenv('DBNAME')
    db_user = os.getenv('DBUSER')

    # Establish a connection to the database
    # conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_password)
    conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres", password="Envision9")

    # Create a cursor object
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Define the SQL query
    sql_query = """
            SELECT *
            FROM public.job_data
            WHERE start_time >= %s AND end_time <= %s
        """

    # Execute the SQL query
    cur.execute(sql_query, (start_time, end_time))

    # Fetch all rows from the last executed SQL query
    rows = cur.fetchall()

    # Convert the results into a pandas DataFrame
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    # Close the cursor and connection
    cur.close()
    conn.close()

    return df


# -------------- CELL 3 --------------

def add_interval_column(ending_time: str, time_series: pd.DataFrame, account_log: pd) -> pd.DataFrame:
    """
    Adds an interval column to a merged DataFrame based on the timestamps of the event series. It also ensures that
    intervals are correctly calculated across different jobs and hosts. The function assumes that the account_log
    DataFrame has an 'End Time' column and both account_log and time_series DataFrames have a 'Job Id' column.

    Parameters:
    :param ending_time: A string representing the ending time to be compared with timestamps in the DataFrame.
                        The string should be in a format that pandas can convert into a datetime object.

    :param time_series: A pandas DataFrame containing time series data. This DataFrame should at least contain 'Job Id',
                        'Host', 'Event', and 'Timestamp' columns.

    :param account_log: A pandas DataFrame containing account logs. This DataFrame should at least contain 'Job Id' and
                        'End Time' columns. 'End Time' will be converted into datetime format within the function.

    Returns:
    :return: A pandas DataFrame that is the result of merging time_series and account_log DataFrames, dropping
             duplicates based on 'Job Id', 'Host', 'Event' and adding an 'Interval' column. This DataFrame also
             contains the columns 'Job Id', 'Host', 'Event', 'Value', 'Units', 'Timestamp', 'Interval'. The 'Interval'
             column represents the time difference between consecutive events, considering 'Job Id', 'Host', and 'Event'.
    """
    # Convert the 'End Time' column to datetime
    account_log['End Time'] = pd.to_datetime(account_log['End Time'])

    # Merge both dataframes on the 'Job Id' column
    merged_df = pd.merge(time_series, account_log, on='Job Id', how='inner')

    # Drop duplicates based on the columns that will form the multi-index
    merged_df = merged_df.drop_duplicates(subset=['Job Id', 'Host', 'Event'])

    # Now we set a multi-index
    merged_df.set_index(['Job Id', 'Host', 'Event'], inplace=True)
    merged_df.sort_index(inplace=True)

    merged_df['Interval'] = merged_df.groupby(level=[0, 1, 2])['Timestamp'].diff().shift(-1).dt.total_seconds()

    end_time_of_each_job = merged_df['End Time'].values
    is_same_sample = merged_df.index.to_series().shift() == merged_df.index.to_series()

    # Align series based on the index before performing subtraction
    end_time_series = pd.Series(end_time_of_each_job, index=merged_df.index)
    na_intervals_timestamp = merged_df.loc[merged_df['Interval'].isnull(), 'Timestamp']
    aligned_end_time_series, aligned_na_intervals_timestamp = end_time_series.align(na_intervals_timestamp)

    # Update the 'Interval' column where 'Interval' is NaN
    merged_df.loc[merged_df['Interval'].isnull(), 'Interval'] = (
            aligned_end_time_series - aligned_na_intervals_timestamp
    ).dt.total_seconds()

    # Where it's not the same sample, replace w/ minimum between Interval and difference between Z2 and current time
    merged_df.loc[~is_same_sample, 'Interval'] = np.minimum(
        merged_df.loc[~is_same_sample, 'Interval'],
        (pd.to_datetime(ending_time) - merged_df.loc[~is_same_sample, 'Timestamp']).dt.total_seconds()
    )

    merged_df = merged_df.reset_index()[['Job Id', 'Host', 'Event', 'Value', 'Units', 'Timestamp', 'Interval']]

    return merged_df


# -------------- CELL 4 --------------
def setup_widgets(unit_values: dict, value):
    """
    Sets up interactive widgets for entering a low and a high value for a given parameter.
    It also sets up a button to save these values. On button click, the entered values are saved
    to a given dictionary and the button's description changes to indicate the values are saved.

    Parameters:
    :param unit_values: The dictionary where the entered values are saved.
    :param value: The name of the parameter for which the values are being entered.

    Returns:
    None. This function doesn't return anything; it creates and displays interactive widgets in a Jupyter notebook.
    """
    value_range = widgets.FloatRangeSlider(
        value=[0.01, 99.99],
        min=0,
        max=100,
        step=0.01,
        orientation='horizontal',
        readout=False,
        description=f'{value} Range:',
        disabled=False,
        style={'description_width': 'initial'},
        layout=widgets.Layout(width="99%")
    )
    range_low_text = widgets.FloatText(layout=widgets.Layout(width="50%"))
    range_high_text = widgets.FloatText(layout=widgets.Layout(width="50%"))

    labels = widgets.HBox(
        [
            widgets.Box([widgets.Label("Low Value:"), range_low_text],
                        layout=widgets.Layout(justify_content="space-around", width="30%")),
            widgets.Box([widgets.Label("High Value:"), range_high_text],
                        layout=widgets.Layout(justify_content="space-between", width="30%"))
        ],
        layout=widgets.Layout(justify_content="space-between"))

    container = widgets.VBox(
        [value_range, labels],
        layout=widgets.Layout(width="50%")
    )
    ipywidgets.dlink((value_range, 'value'), (range_low_text, 'value'), transform=lambda x: x[0])
    ipywidgets.dlink((range_low_text, 'value'), (value_range, 'value'), transform=lambda x: (x, value_range.value[1]))
    ipywidgets.dlink((value_range, 'value'), (range_high_text, 'value'), transform=lambda x: x[1])
    ipywidgets.dlink((range_high_text, 'value'), (value_range, 'value'), transform=lambda x: (value_range.value[0], x))
    display(container)
    button = widgets.Button(description="Save Values")
    display(button)

    def on_button_clicked_save(b):
        unit_values[value] = value_range
        b.description = "Values Saved!"
        b.button_style = 'success'  # The button turns green when clicked

    button.on_click(on_button_clicked_save)


# -------------- CELL 5 --------------
def get_timeseries_by_values_and_unit(units: dict, ts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the input dataframe to return only those rows where the 'unit' column matches the keys
    in the input dictionary and the 'value' column is within the range specified by the corresponding
    value in the dictionary.

    Parameters:
    :param units: A dictionary where keys are the units to be matched and values are tuples indicating
    the inclusive range of acceptable values for the 'value' column.
    :param ts_df: The input pandas DataFrame which contains the columns 'unit' and 'value' among others.

    Returns:
    A pandas DataFrame that contains only the rows from the input dataframe that meet the criteria specified
    by the 'units' dictionary.
    """
    # Check that units dictionary is not empty
    if not units:
        raise ValueError("The units dictionary is empty.")

    # Check that DataFrame is not empty and contains necessary columns
    if ts_df.empty or 'unit' not in ts_df.columns or 'value' not in ts_df.columns:
        raise ValueError("DataFrame is either empty or does not contain 'unit' and 'value' columns.")

    df_list = []

    # Loop over the dictionary
    for unit, range_slider in units.items():
        # Get the range value from the widget
        value_range = range_slider.value

        # Check that the range value is a valid tuple of two floats
        if not isinstance(value_range, tuple) or len(value_range) != 2 or not all(
                isinstance(i, float) for i in value_range):
            raise ValueError(f"Invalid range value for unit '{unit}': {value_range}. Must be a tuple of two floats.")

        # Filter the DataFrame
        print(f"Filtering Dataframe for {unit}. . .")
        mask = (ts_df['unit'] == unit) & (ts_df['value'] >= value_range[0]) & (ts_df['value'] <= value_range[1])
        filtered_df = ts_df.loc[mask]
        df_list.append(filtered_df)

    # Concatenate all the filtered DataFrames and remove duplicates
    result = pd.concat(df_list).drop_duplicates()
    return result


def get_timeseries_by_hosts(hosts: str, incoming_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the incoming DataFrame to only include rows where the value in the 'Host' column matches one of the hosts.

    Parameters:
    :param hosts: A string of host names separated by commas. Special characters will be removed and the string will be
    converted to upper case.
    :param incoming_dataframe: A pandas DataFrame that includes a column labeled 'Host'.

    Returns:
    :return pd.DataFrame: A DataFrame filtered to only include rows whose 'Host' value matches one of the host names.
    """
    hosts = remove_special_chars(hosts)
    hosts = hosts.upper()
    if not isinstance(hosts, list):
        hosts = hosts.replace(" ", "").split(",")

    # This is using the isin function which checks each value in the 'Host' column to see if it's in the hosts list
    return incoming_dataframe[incoming_dataframe['host'].isin(hosts)]


def get_timeseries_by_job_ids(job_ids: str, incoming_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the incoming DataFrame to only include rows where the value in the 'Job Id' column matches one of the
    job ids.

    Parameters:
    :param job_ids: A string of job ids separated by commas. Special characters will be removed and the string will be
    converted to upper case.
    :param incoming_dataframe: A pandas DataFrame that includes a column labeled 'Job Id'.

    Returns:
    :return pd.DataFrame: A DataFrame filtered to only include rows whose 'Job Id' value matches one of the job ids.
    """
    job_ids = remove_special_chars(job_ids)
    job_ids = job_ids.upper()

    if not isinstance(job_ids, list):
        job_ids = job_ids.replace(" ", "").split(",")

    # This is using the isin function which checks each value in the 'Job Id' column to see if it's in the job_ids list
    return incoming_dataframe[incoming_dataframe['jid'].isin(job_ids)]


def get_account_logs_by_job_ids(time_series: pd.DataFrame, account_log: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the provided account_log DataFrame to only include rows where the 'Job Id' is also found in the time_series DataFrame.

    Parameters:
    :param time_series: A pandas DataFrame that contains a column 'Job Id'. These job ids are used to filter the account_log DataFrame.
    :param account_log: A pandas DataFrame that contains a column 'Job Id'. The rows of account_log DataFrame are filtered based on the 'Job Id' column.

    Returns:
    :return: A pandas DataFrame which is a subset of the provided account_log DataFrame. It only contains the rows where 'Job Id'
             is also found in the time_series DataFrame.
    """
    jobs = time_series['jid'].to_list()

    return account_log[account_log['jid'].isin(jobs)]


def filter_return_columns(ts_return_col: list, time_series: pd.DataFrame) -> pd.DataFrame:
    """
    This function filters the incoming timeseries DataFrame such that the returned DataFrame only contains those columns
    that are included in the ts_return_col argument.

    Parameters:
    :param ts_return_col: A list of column names that should be included in the returned DataFrame.
    :param time_series: The input pandas DataFrame from which columns should be selected.

    Returns:
    A pandas DataFrame that contains only the columns from the input DataFrame that are listed in ts_return_col.
    """

    # Check that the input is a DataFrame
    if not isinstance(time_series, pd.DataFrame):
        raise TypeError(f"Input 'time_series' is not a DataFrame. Received {type(time_series).__name__} instead.")

    # Check that the DataFrame is not empty
    if time_series.empty:
        raise ValueError("Input DataFrame is empty.")

    # Check if ts_return_col is a list
    if not isinstance(ts_return_col, list):
        raise TypeError(f"Input 'ts_return_col' is not a list. Received {type(ts_return_col).__name__} instead.")

    # Check if all elements in ts_return_col are in the DataFrame's columns
    if not set(ts_return_col).issubset(set(time_series.columns)):
        missing_cols = set(ts_return_col) - set(time_series.columns)
        raise ValueError(f"Columns {missing_cols} are not present in the input DataFrame.")

    # Return a DataFrame containing only the desired columns
    return time_series[ts_return_col]


def create_csv_download_link(df, title=None, filename="data.csv"):
    """
    Generates a link to download a DataFrame as a CSV file.

    This function converts a DataFrame to a CSV format, encodes it in base64, and embeds it into an HTML link that enables users to download the data.

    Parameters:
    :param df: A pandas DataFrame that is to be downloaded.
    :param title: The text to display for the download link. Defaults to None.
    :param filename: The name to use for the downloaded file. Defaults to "data.csv".

    Returns:
    :return: An IPython.core.display.HTML object that contains an HTML string. When displayed in an IPython environment (like Jupyter), it manifests as a clickable download link.
    """
    csv = df.to_csv()
    b64 = base64.b64encode(csv.encode())
    payload = b64.decode()
    html = '<a download="{filename}" href="data:text/csv;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload, title=title, filename=filename)
    return HTML(html)


def create_excel_download_link(df, title=None, filename="data.xlsx"):
    """
    Generates a link to download a DataFrame as an Excel (.xlsx) file.

    This function converts a DataFrame to an Excel file format, encodes it in base64, and embeds it into an HTML link that enables users to download the data.

    Parameters:
    :param df: A pandas DataFrame that is to be downloaded.
    :param title: The text to display for the download link. If not provided, the link will not have a descriptive text.
    :param filename: The name to use for the downloaded file. Defaults to "data.xlsx".

    Returns:
    :return: An IPython.core.display.HTML object that contains an HTML string. When displayed in an IPython environment (like Jupyter), it manifests as a clickable download link.
    """
    output = io.BytesIO()
    # Write the DataFrame to the BytesIO object as an Excel file
    df.to_excel(output, engine='xlsxwriter', sheet_name='Sheet1')
    # Get the Excel file data
    excel_data = output.getvalue()
    # Encode the Excel file data to base64
    b64 = base64.b64encode(excel_data)
    # Create the payload
    payload = b64.decode()
    # Create the HTML link
    html = '<a download="{filename}" href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{payload}" target="_blank">{title}</a>'
    html = html.format(payload=payload, title=title, filename=filename)
    # Return the HTML link
    return HTML(html)


# -------------- CELL 6 --------------
# Aryamaan
def get_average(time_series: pd.DataFrame, rolling=False, window=None) -> pd.DataFrame:
    """
    Calculates the median of the provided time_series DataFrame, either on the entire DataFrame or on a rolling window
    basis.

    Parameters:
    :param time_series: A pandas DataFrame that contains a column 'Value'. The median is calculated on the 'Value'
                        column.
    :param rolling: A boolean indicating whether the median should be calculated for the entire DataFrame (False) or
                    on a rolling window basis (True).
    :param window: The size of the rolling window for which the median is to be calculated. It should be specified
                   in string format like use D for days, H for hours, T for minutes, and S for seconds. This parameter
                   is considered only if rolling is set to True.

    Returns:
    :return: A pandas DataFrame or Series which contains the median of the 'Value' column of the provided time_series
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
    :param time_series: A pandas DataFrame that contains a column 'Value'. The median is calculated on the 'Value'
                        column.
    :param rolling: A boolean indicating whether the median should be calculated for the entire DataFrame (False) or
                    on a rolling window basis (True).
    :param window: The size of the rolling window for which the median is to be calculated. It should be specified
                   in string format like use D for days, H for hours, T for minutes, and S for seconds. This parameter
                   is considered only if rolling is set to True.

    Returns:
    :return: A pandas DataFrame or Series which contains the median of the 'Value' column of the provided time_series
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
    :param time_series: A pandas DataFrame that contains a column 'Value'. The standard deviation is calculated on the
                        'Value' column.
    :param rolling: A boolean indicating whether the standard deviation should be calculated for the entire DataFrame
                    (False) or on a rolling window basis (True).
    :param window: The size of the rolling window for which the standard deviation is to be calculated. It should be
                    specified in string format like use D for days, H for hours, T for minutes, and S for seconds. This
                    parameter is considered only if rolling is set to True.

    Returns:
    :return: A pandas DataFrame or Series which contains the standard deviation of the 'Value' column of the provided
            time_series DataFrame. If rolling is set to True, the result will be a DataFrame with the rolling window
            standard deviation. If rolling is False, the result will be a Series with the overall standard deviation.
    """
    result = time_series.copy()
    result.drop(['jid', 'host', 'event', 'unit'], axis=1, inplace=True)

    if rolling:
        return result.rolling(window=window).std()

    return result.std()


def plot_choices(stats_value, rolling: bool, df_avg: pd.DataFrame, df_mean: pd.DataFrame, df_median: pd.DataFrame,
                 df_std: pd.DataFrame, start, end):
    """
    This function plots various statistics (average, mean, median, and standard deviation) of a time series data.
    It can either print the statistic value over a given period (non-rolling) or plot the rolling statistics over time.

    Parameters:
    :param stats_value: A list of strings indicating which statistics to consider. Valid entries include 'Average',
                        'Mean', 'Median', and 'Standard Deviation'.
    :param rolling: A boolean indicating whether to print the statistics (False) or plot them over time (True).
    :param df_avg: A pandas DataFrame that contains a column 'Value' with the average values of the time series data.
    :param df_mean: A pandas DataFrame that contains a column 'Value' with the mean values of the time series data.
    :param df_median: A pandas DataFrame that contains a column 'Value' with the median values of the time series data.
    :param df_std: A pandas DataFrame that contains a column 'Value' with the standard deviation values of the time series data.
    :param start: A datetime object that represents the start time of the data to be considered.
    :param end: A datetime object that represents the end time of the data to be considered.

    Returns:
    :return: This function does not return anything. Instead, it prints the statistics over a given period if rolling
             is False or creates a plot of the rolling statistics over time if rolling is True. The type of statistics
             plotted depends on the choices provided in the 'stats_value' parameter.
    """
    time_format = '%Y-%m-%d %H:%M:%S'

    for choice in stats_value:
        if choice == "Average":
            if not rolling:
                print(
                    f"{choice}: {df_avg['Value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_avg['Value'].plot(label='Average', color='b')
        if choice == "Mean":
            if not rolling:
                print(
                    f"{choice}: {df_mean['Value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_mean['Value'].plot(label='Mean', color='g')
        if choice == "Median":
            if not rolling:
                print(
                    f"{choice}: {df_median['Value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_median['Value'].plot(label='Median', color='r')
        if choice == "Standard Deviation":
            if not rolling:
                print(
                    f"{choice}: {df_std['Value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_std['Value'].plot(label='Std Dev', color='k')


def plot_box_and_whisker(df_avg: pd.DataFrame, df_mean: pd.DataFrame, df_std: pd.DataFrame, df_median: pd.DataFrame):
    """
    This function generates a box and whisker plot for various statistics (average, mean, median, and standard deviation)
    of a time series data, as contained within provided DataFrames.

    Parameters:
    :param df_avg: A pandas DataFrame that contains a column 'Value' with the average values of the time series data.
    :param df_mean: A pandas DataFrame that contains a column 'Value' with the mean values of the time series data.
    :param df_std: A pandas DataFrame that contains a column 'Value' with the standard deviation values of the time series data.
    :param df_median: A pandas DataFrame that contains a column 'Value' with the median values of the time series data.

    Returns:
    :return: This function does not return anything. Instead, it plots a box and whisker plot for the provided
             statistics. Each box in the plot represents one of the statistics. The box extends from the lower to
             upper quartile values of the data, with a line at the median. The whiskers extend from the box to show
             the range of the data. Outlier points are those past the end of the whiskers.
    """
    print("Generating box and whisker plot. . .")
    # Collect statistics into a list of pandas Series or numpy arrays
    all_data = []
    labels = []
    color_choices = []
    if not df_avg.empty:
        df_avg.dropna(subset=['Value'], inplace=True)
        all_data.append(df_avg['Value'])
        labels.append('Average')
        color_choices.append('pink')
    if not df_median.empty:
        df_median.dropna(subset=['Value'], inplace=True)
        all_data.append(df_median['Value'])
        labels.append('Median')
        color_choices.append('lightgreen')
    if not df_std.empty:
        df_std.dropna(subset=['Value'], inplace=True)
        all_data.append(df_std['Value'])
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


def plot_rolling_basic_stats(interval_type_value: str, time_value, time_units_value, unit_values: dict):
    """
    This function generates a plot for rolling basic statistics of a time series data. The function allows the rolling
    window to be specified either as a fixed count of data points or as a fixed time interval. The y-axis represents
    the units of measure for the time series data, which can be multiple.

    Parameters:
    :param interval_type_value: A string that specifies the type of interval for the rolling window. Valid values
                                are "Count" for a fixed count of data points and "Time" for a fixed time interval.
    :param time_value: If interval_type_value is "Count", this is the count of data points for the rolling window. If
                       interval_type_value is "Time", this is the length of the time interval for the rolling window.
    :param time_units_value: If interval_type_value is "Time", this specifies the units of the time interval. Valid values
                             are "Days", "Hours", "Minutes", and "Seconds". This parameter is ignored if
                             interval_type_value is "Count".
    :param unit_values: A dictionary where the keys are the units of measure for the time series data. The dictionary values
                        are not used by this function.

    Returns:
    :return: This function does not return anything. Instead, it creates a plot with the x-axis representing the rolling
             window and the y-axis representing the units of measure for the time series data. The plot style is set
             to 'fivethirtyeight', which is a popular matplotlib style, known for its clear and clean aesthetics.
    """
    print("Plotting time series stats . . .")
    time_map = {'Days': 'D', 'Hours': 'H', 'Minutes': 'T', 'Seconds': 'S'}

    x_axis_label = ""
    if interval_type_value == "Count":
        x_axis_label += f"Timestamp - Rolling Window: {time_value:,} Rows"
    elif interval_type_value == "Time":
        x_axis_label += f"Timestamp - Rolling Window: {time_value}{time_map[time_units_value]}"

    y_axis_label = ""
    if len(unit_values) > 0:
        for key in unit_values.keys():
            y_axis_label += f"{key} "

    plt.gcf().autofmt_xdate()  # auto formats datetimes
    plt.style.use('fivethirtyeight')
    plt.legend(loc='upper left', fontsize="10")
    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    plt.grid()
    plt.show()


def plot_pdf(ts_df: pd.DataFrame):
    """
    This function generates a Probability Density Function (PDF) plot for a given time series DataFrame.

    Parameters:
    :param ts_df: A pandas DataFrame that contains a column 'Value'. This column should represent the time series
                  data for which the PDF will be calculated and plotted.

    Returns:
    :return: This function does not return anything. Instead, it creates a histogram and overlaid kernel density
             estimate plot using seaborn's histplot function. The x-axis of the plot represents the 'Value' column
             from the input DataFrame, and the y-axis represents the estimated probability density. The title of
             the plot is 'Probability Density Function (PDF)'.
    """
    time_series_df = ts_df.dropna()
    sns.histplot(time_series_df['Value'], kde=True)
    plt.title('Probability Density Function (PDF)')
    plt.show()


def plot_cdf(ts_df: pd.DataFrame):
    """
    This function generates a Cumulative Distribution Function (CDF) plot for a given time series DataFrame.

    Parameters:
    :param ts_df: A pandas DataFrame that contains a column 'Value'. This column should represent the time series
                  data for which the CDF will be calculated and plotted.

    Returns:
    :return: This function does not return anything. Instead, it creates a step plot using seaborn's histplot function,
             with the cumulative option set to True. The x-axis of the plot represents the 'Value' column from the
             input DataFrame, and the y-axis represents the cumulative frequency. The title of the plot is
             'Cumulative Distribution Function (CDF)'.
    """
    time_series_df = ts_df.dropna()
    sns.histplot(time_series_df['Value'], cumulative=True, element="step", fill=False)
    plt.title('Cumulative Distribution Function (CDF)')
    plt.show()


def plot_data_points_outside_threshold(ratio_threshold_value, ts_df: pd.DataFrame):
    threshold = ratio_threshold_value

    # Here we calculate the ratio of data outside the threshold
    num_data_points = len(ts_df['Value'])
    num_outside_threshold = sum(abs(ts_df['Value']) > threshold)

    ratio_outside_threshold = num_outside_threshold / num_data_points

    # Plotting
    plt.bar(['Inside Threshold', 'Outside Threshold'], [1 - ratio_outside_threshold, ratio_outside_threshold])
    plt.title('Ratio of Data Outside Threshold')
    plt.ylabel('Ratio')
    plt.show()


#     print("Plotting data with threshold band . . .")
#     threshold = ratio_threshold.value
# fig, ax = plt.subplots() ax.plot(time_series_df.index, time_series_df['Value'], label='Data', color='tab:blue')
# ax.fill_between(time_series_df.index, threshold, time_series_df['Value'].where(time_series_df['Value']>=threshold),
# where=(time_series_df['Value']>=threshold), color='tab:red', alpha=0.2, label='Above Threshold') ax.fill_between(
# time_series_df.index, -threshold, time_series_df['Value'].where(time_series_df['Value']<=-threshold),
# where=(time_series_df['Value']<=-threshold), color='tab:red', alpha=0.2) ax.axhline(threshold, color='tab:green',
# linestyle='--', label='Threshold') ax.axhline(-threshold, color='tab:green', linestyle='--')
#     ax.set_title('Data with Threshold Band')
#     ax.set_ylabel('Value')
#     ax.legend(loc='upper right')
#     plt.show()


# -------------- CELL 8 --------------

def calculate_and_plot_correlation(time_series: pd.DataFrame, correlations):
    """
    This function calculates the Pearson Correlation Coefficient between two time series.

    Parameters:
    time_series: A pandas DataFrame that contains a column 'Value'. This column should represent the time series
                 data for which the Pearson Correlation Coefficient will be calculated.
    correlations: A tuple of two elements, each representing the metric to be used for correlation calculation.

    Returns:
    This function does not return anything. Instead, it prints the Pearson Correlation Coefficient
    between the two time series.
    """
    metric_one, metric_two = correlations

    # The duplicated function returns a boolean Series denoting duplicate index values, and
    # the ~ operator is used to invert the boolean values. This way we're keeping only the
    # rows where the index is not duplicated
    ts_metric_one = time_series[time_series['Event'] == metric_one]
    ts_metric_one = ts_metric_one[~ts_metric_one.index.duplicated(keep='first')]

    ts_metric_two = time_series[time_series['Event'] == metric_two]
    ts_metric_two = ts_metric_two[~ts_metric_two.index.duplicated(keep='first')]

    # find common timestamps using index intersection
    common_timestamps = ts_metric_one.index.intersection(ts_metric_two.index)

    metric_one_values = ts_metric_one.loc[common_timestamps, 'Value'].values
    metric_two_values = ts_metric_two.loc[common_timestamps, 'Value'].values

    # Check if both lists have same length before calculating correlation
    if len(metric_one_values) == len(metric_two_values):
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
    else:
        print(f'The two metrics do not have the same amount of sampling in the data, '
              f'one is {len(metric_one_values)}, and the other is {len(metric_two_values)}')


# -------------- CELL 9 --------------
# Function to generate download link
def create_download_file_link(file_list):
    links = []
    for file_name in file_list:
        file_path = os.path.join("./", file_name)  # Adjust the path when we know where they'll be
        if os.path.isfile(file_path):
            links.append(FileLink(file_path))
        else:
            print(f"File {file_name} does not exist.")
    return links


# Function to be linked to the button, to execute the file download.
def on_download_button_clicked(b, widget_value):
    download_files = widget_value.value
    if download_files:
        links = create_download_file_link(download_files)
        for link in links:
            display(link)
    else:
        print("No file selected for download.")
