

def get_time_series_from_database(start_time, end_time) -> Optional[pd.DataFrame]:
    """
    This function retrieves data from a database within a specified time range and returns it as a pandas DataFrame.

    The function first establishes a connection to the database, then uses a SQL query to select all data from the
    'public.host_data' table where the 'time' is within the provided start and end times.

    :param start_time: The start time in the format of '%Y-%m-%d %H:%M:%S'.
    :param end_time: The end time in the format of '%Y-%m-%d %H:%M:%S'.
    :return: A pandas DataFrame containing the data or None if there's an error during database operations.
    """
    try:
        conn = get_database_connection()
        if conn is None:
            raise ValueError('Database connection could not be established.')

        # Create a cursor object
        cur = conn.cursor(cursor_factory=extras.DictCursor)

        # Define the SQL query
        sql_query = """
                SELECT jid, host, event, value, unit, time
                FROM public.host_data
                WHERE time >= %s AND time <= %s
            """

        # Execute the SQL query
        cur.execute(sql_query, (start_time, end_time))

        # Fetch all rows from the last executed SQL query
        rows = cur.fetchall()

        # Convert the results into a pandas DataFrame
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"An error occurred: {error}")
        return None

    finally:
        # Ensure resources are released properly even if an error occurred
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def get_account_log_from_database(start_time, end_time) -> Optional[pd.DataFrame]:
    """
    Retrieves account data from a database within a specified time range and returns it as a pandas DataFrame.

    The function first establishes a connection to the database, then uses a SQL query to select all data from the
    'public.job_data' table where the 'start_time' and 'end_time' are within the provided start and end times.

    :param start_time: The start time in the format of '%Y-%m-%d %H:%M:%S'.
    :param end_time: The end time in the format of '%Y-%m-%d %H:%M:%S'.
    :return: A pandas DataFrame containing the data or None if there's an error during database operations.
    """
    conn = None
    cur = None

    try:
        conn = get_database_connection()
        if conn is None:
            raise ValueError('Database connection could not be established.')

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

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"An error occurred: {error}")
        return None

    finally:
        # Ensure resources are released properly even if an error occurred
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def add_interval_column(ending_time: str, time_series: pd.DataFrame, account_log: pd) -> pd.DataFrame:
    """
    Adds an interval column to a merged DataFrame based on the timestamps of the event series. It also ensures that
    intervals are correctly calculated across different jobs and hosts. The function assumes that the account_log
    DataFrame has an 'end_time' column and both account_log and time_series DataFrames have a 'jid' column.

    This function converts the 'end_time' column in the account_log DataFrame and the 'time' column in the time_series
    DataFrame to timezone-naive datetime if necessary. It also converts the ending_time argument to timezone-naive
    datetime.

    Parameters:
    :param ending_time: A string representing the ending time to be compared with timestamps in the DataFrame.
                        The string should be in a format that pandas can convert into a datetime object.

    :param time_series: A pandas DataFrame containing time series data. This DataFrame should at least contain 'jid',
                        'host', 'event', and 'time' columns.

    :param account_log: A pandas DataFrame containing account logs. This DataFrame should at least contain 'jid' and
                        'end_time' columns. 'end_time' will be converted into datetime format within the function.

    Returns:
    :return: A pandas DataFrame that is the result of merging time_series and account_log DataFrames, dropping
             duplicates based on 'jid', 'host', 'event' and adding an 'interval' column. This DataFrame also
             contains the columns 'jid', 'host', 'event', 'value', 'unit', 'time', 'interval'. The 'interval'
             column represents the time difference between consecutive events, considering 'jid', 'host', and 'event'.
    """
    # Convert the 'end_time' column to datetime
    account_log['end_time'] = pd.to_datetime(account_log['end_time'])
    # Convert the 'end_time' column to timezone-naive if necessary
    if pd.api.types.is_datetime64tz_dtype(account_log['end_time']):
        account_log['end_time'] = account_log['end_time'].dt.tz_convert(None)

    # Merge both dataframes on the 'jid' column
    merged_df = pd.merge(time_series, account_log, on='jid', how='inner')

    # Drop duplicates based on the columns that will form the multi-index
    merged_df = merged_df.drop_duplicates(subset=['jid', 'host', 'event'])

    # Now we set a multi-index
    merged_df.set_index(['jid', 'host', 'event'], inplace=True)
    merged_df.sort_index(inplace=True)

    merged_df['interval'] = merged_df.groupby(level=[0, 1, 2])['time'].diff().shift(-1).dt.total_seconds()

    end_time_of_each_job = merged_df['end_time'].values
    is_same_sample = merged_df.index.to_series().shift() == merged_df.index.to_series()

    # Align series based on the index before performing subtraction
    end_time_series = pd.Series(end_time_of_each_job, index=merged_df.index)
    na_intervals_timestamp = merged_df.loc[merged_df['interval'].isnull(), 'time']

    # Convert the 'time' column to timezone-naive if necessary
    if pd.api.types.is_datetime64tz_dtype(na_intervals_timestamp):
        na_intervals_timestamp = na_intervals_timestamp.dt.tz_convert(None)

    aligned_end_time_series, aligned_na_intervals_timestamp = end_time_series.align(na_intervals_timestamp)

    # Update the 'interval' column where 'interval' is NaN
    merged_df.loc[merged_df['interval'].isnull(), 'interval'] = (
            aligned_end_time_series - aligned_na_intervals_timestamp
    ).dt.total_seconds()

    # Make sure 'ending_time' is timezone-naive
    ending_time = pd.to_datetime(ending_time).tz_localize(None)

    # Make sure 'time' column of 'merged_df' is timezone-naive
    if pd.api.types.is_datetime64tz_dtype(merged_df['time']):
        merged_df['time'] = merged_df['time'].dt.tz_convert(None)

    # Where it's not the same sample, replace w/ minimum between Interval and difference between Z2 and current time
    merged_df.loc[~is_same_sample, 'interval'] = np.minimum(
        merged_df.loc[~is_same_sample, 'interval'],
        (ending_time - merged_df.loc[~is_same_sample, 'time']).dt.total_seconds()
    )

    merged_df = merged_df.reset_index()[['jid', 'host', 'event', 'value', 'unit', 'time', 'interval']]

    return merged_df


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
    by the 'unit' dictionary.
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
        mask = (ts_df['unit'] == unit) & (ts_df['value'] >= value_range[0]) & (ts_df['value'] <= value_range[1])
        filtered_df = ts_df.loc[mask]
        df_list.append(filtered_df)

    # Concatenate all the filtered DataFrames and remove duplicates
    result = pd.concat(df_list).drop_duplicates()
    return result


def get_timeseries_by_hosts(hosts: str, incoming_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the incoming DataFrame to only include rows where the value in the 'host' column matches one of the hosts.

    Parameters:
    :param hosts: A string of host names separated by commas. Special characters will be removed and the string will be
    converted to upper case.
    :param incoming_dataframe: A pandas DataFrame that includes a column labeled 'host'.

    Returns:
    :return pd.DataFrame: A DataFrame filtered to only include rows whose 'host' value matches one of the host names.
    """
    hosts = remove_special_chars(hosts)
    hosts = hosts.upper()
    if not isinstance(hosts, list):
        hosts = hosts.replace(" ", "").split(",")

    # This is using the isin function which checks each value in the 'host' column to see if it's in the hosts list
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


def plot_choices(stats_value, rolling: bool, df_avg: pd.DataFrame, df_mean: pd.DataFrame, df_median: pd.DataFrame,
                 df_std: pd.DataFrame, start, end):
    """
    This function plots various statistics (average, mean, median, and standard deviation) of a time series data.
    It can either print the statistic value over a given period (non-rolling) or plot the rolling statistics over time.

    Parameters:
    :param stats_value: A list of strings indicating which statistics to consider. Valid entries include 'Average',
                        'Mean', 'Median', and 'Standard Deviation'.
    :param rolling: A boolean indicating whether to print the statistics (False) or plot them over time (True).
    :param df_avg: A pandas DataFrame that contains a column 'value' with the average values of the time series data.
    :param df_mean: A pandas DataFrame that contains a column 'value' with the mean values of the time series data.
    :param df_median: A pandas DataFrame that contains a column 'value' with the median values of the time series data.
    :param df_std: A pandas DataFrame that contains a column 'value' with the standard deviation values of the time series data.
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
                    f"{choice}: {df_avg['value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_avg['value'].plot(label='Average', color='b')
        if choice == "Mean":
            if not rolling:
                print(
                    f"{choice}: {df_mean['value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_mean['value'].plot(label='Mean', color='g')
        if choice == "Median":
            if not rolling:
                print(
                    f"{choice}: {df_median['value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_median['value'].plot(label='Median', color='r')
        if choice == "Standard Deviation":
            if not rolling:
                print(
                    f"{choice}: {df_std['value']} for {start.strftime(time_format)} to {end.strftime(time_format)}")
            else:
                df_std['value'].plot(label='Std Dev', color='k')


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