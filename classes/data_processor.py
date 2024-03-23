from scipy.stats import pearsonr
import pandas as pd
import zipfile
import re
import io
import os


class DataProcessor:
    def __init__(self, base_widget_manager):
        self.base_widget_manager = base_widget_manager

    def remove_special_chars(self, s: str) -> str:
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

    def validate_jid(self, value):
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
                return "Error: For 'jid', value must be a comma-separated list of strings starting with 'JOB' " \
                       "followed by one or more digits."
        return None

    def validate_numeric_columns(self, column, value):
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

    def validate_account(self, value):
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
                return "Error: For 'account', value must be a comma-separated list of strings starting with " \
                       "'GROUP' followed by one or more digits."
        return None

    def validate_username(self, value):
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
                return "Error: For 'username', value must be a comma-separated list of strings starting with " \
                       "'USER' followed by one or more digits."
        return None

    def validate_host_list(self, value):
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
                return "Error: For 'host_list', value must be a comma-separated list of strings starting with " \
                       "'NODE' followed by one or more digits."
        return None

    def validate_jobname(self, value):
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
                return "Error: For job name, value must be a comma-separated list of strings starting with " \
                       "'JOBNAME' followed by one or more digits."
        return None

    def validate_condition_jobs(self, column, value):
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
            error_message = self.validate_jid(value)
        elif column in ['ncores', 'ngpus', 'nhosts', 'timelimit']:
            error_message = self.validate_numeric_columns(column, value)
        elif column == 'account':
            error_message = self.validate_account(value)
        elif column == 'username':
            error_message = self.validate_username(value)
        elif column == 'host_list':
            error_message = self.validate_host_list(value)
        elif column == 'jobname':
            error_message = self.validate_jobname(value)
        return error_message

    # Validate condition
    def validate_condition_hosts(self, column, value):
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
            error_message = self.validate_event(value)
        elif column == 'host':
            error_message = self.validate_host(value)
        elif column == 'jid':
            error_message = self.validate_jid(value)
        elif column == 'unit':
            error_message = self.validate_unit(value)
        elif column == 'value':
            error_message = self.validate_value(value)
        return error_message

    def validate_event(self, value):
        """
        Validates the provided event value. The function checks if the value is in the predefined list.

        Parameters:
        :param value: A string containing the event that needs to be validated.

        Returns:
        :return: An error message string if the value is not in the predefined list for the event.
                 Returns None if the value is valid.
        """
        if value not in ['cpuuser', 'block', 'memused', 'memused_minus_diskcache', 'gpu_usage', 'nfs']:
            return "Error: For 'event', value must be one of: cpuuser, block, memused, memused_minus_diskcache, " \
                   "gpu_usage, nfs."
        return None

    def validate_host(self, value):
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
                return "Error: For 'host', value must be a comma-separated list of strings starting with 'NODE' " \
                       "followed by one or more digits."
        return None

    def validate_unit(self, value):
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

    def validate_value(self, value):
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

    def validate_in_input(self, values_str):
        """
        Validates and parses the comma-separated string for the IN SQL functionality.

        Args:
        - values_str (str): A comma-separated string.

        Returns:
        - list: A list of parsed values.
        """
        # Strip whitespace and split by comma
        values = [val.strip() for val in values_str.split(',')]
        return values

    def construct_query_hosts(self, where_conditions_hosts, host_data_columns_dropdown, validate_button_hosts,
                              start_time_hosts, end_time_hosts):
        selected_columns = ', '.join(host_data_columns_dropdown)
        table_name = 'host_data'

        # Handle DISTINCT
        if self.base_widget_manager.distinct_checkbox.value:
            query = f"SELECT DISTINCT {selected_columns} FROM {table_name}"
        else:
            query = f"SELECT {selected_columns} FROM {table_name}"

        # Initialize params and local conditions list
        params = []
        local_conditions = where_conditions_hosts.copy()  # Start with the conditions passed in

        # Add the values from where_conditions_values to params
        params.extend(self.base_widget_manager.where_conditions_values)

        # Handle time validation
        if validate_button_hosts == "Times Valid":
            local_conditions.append(("time", "BETWEEN", "%s AND %s"))
            params.extend([start_time_hosts, end_time_hosts])

        # Handle IN condition
        in_values = [value.strip() for value in self.base_widget_manager.in_values_textarea.value.split(',')]
        if in_values and in_values[0]:  # Check if the first value is not empty
            in_clause = ', '.join(['%s'] * len(in_values))
            local_conditions.append((self.base_widget_manager.in_values_dropdown.value, "IN", f"({in_clause})"))
            params.extend(in_values)

        # Construct the WHERE clause
        if local_conditions:
            where_clause = " AND ".join([f"{col} {op} {val}" for col, op, val in local_conditions])
            query += f" WHERE {where_clause}"

        # Handle ORDER BY
        if self.base_widget_manager.order_by_dropdown.value != 'None':
            query += f" ORDER BY {self.base_widget_manager.order_by_dropdown.value} {self.base_widget_manager.order_by_direction_dropdown.value}"

        # Handle LIMIT
        if self.base_widget_manager.limit_input.value > 0:
            query += f" LIMIT {self.base_widget_manager.limit_input.value}"

        return query, params

    def construct_job_data_query(self, where_conditions_jobs, job_data_columns_dropdown, validate_button_jobs,
                                 start_time_jobs,
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
        valid_columns = {'jid', 'submit_time', 'start_time', 'end_time', 'runtime', 'timelimit', 'node_hrs',
                         'nhosts', 'ncores', 'ngpus', 'username', 'account', 'queue', 'state', 'jobname', 'exitcode',
                         'host_list', '*'}
        selected_columns = job_data_columns_dropdown

        # Validate that the selected columns are all in the set of valid columns
        if not all(column in valid_columns for column in selected_columns):
            raise ValueError("Invalid column name selected")

        selected_columns_str = ', '.join(selected_columns)
        table_name = 'job_data'

        if self.base_widget_manager.distinct_checkbox_jobs.value:
            query = f"SELECT DISTINCT {selected_columns_str} FROM {table_name}"
        else:
            query = f"SELECT {selected_columns_str} FROM {table_name}"

        # Initialize params and local conditions list
        params = []
        local_conditions = [(col, op, "%s") for col, op, _ in where_conditions_jobs]

        # Extract values and add to params
        params.extend([val for _, _, val in where_conditions_jobs])

        # Handle time validation
        if validate_button_jobs == "Times Valid":
            local_conditions.append(("start_time", "BETWEEN", "%s AND %s"))
            params.extend([start_time_jobs, end_time_jobs])

        # Handle IN condition
        in_values = [value.strip() for value in self.base_widget_manager.in_values_textarea_jobs.value.split(',')]
        if in_values and in_values[0]:  # Check if the first value is not empty
            in_clause = ', '.join(['%s'] * len(in_values))
            local_conditions.append((self.base_widget_manager.in_values_dropdown_jobs.value, "IN", f"({in_clause})"))
            params.extend(in_values)

        # Construct the WHERE clause
        if local_conditions:
            where_clause = " AND ".join([f"{col} {op} {val}" for col, op, val in local_conditions])
            query += f" WHERE {where_clause}"

        # Handle ORDER BY
        if self.base_widget_manager.order_by_dropdown_jobs.value != 'None':
            query += f" ORDER BY {self.base_widget_manager.order_by_dropdown_jobs.value} {self.base_widget_manager.order_by_direction_dropdown_jobs.value}"

        # Handle LIMIT
        if self.base_widget_manager.limit_input_jobs.value > 0:
            query += f" LIMIT {self.base_widget_manager.limit_input_jobs.value}"

        return query, params

    def get_mean(self, time_series: pd.DataFrame, rolling=False, window=None) -> pd.DataFrame:
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

    def get_median(self, time_series: pd.DataFrame, rolling=False, window=None) -> pd.DataFrame:
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

    def get_standard_deviation(self, time_series: pd.DataFrame, rolling=False, window=None) -> pd.DataFrame:
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

    def remove_columns(self):
        """
       Removes specific columns ('type', 'diff', 'arc') from the given dataframe.

       Parameters:
       :param df: A pandas DataFrame

       Returns:
       :return: A pandas DataFrame with the specified columns removed.
       """
        if not isinstance(self.base_widget_manager.time_series_df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")

        columns_to_remove = ['type', 'diff', 'arc']

        try:
            self.base_widget_manager.time_series_df.drop(columns=columns_to_remove, errors='ignore', inplace=True)
        except Exception as e:
            raise RuntimeError(f"An error occurred while removing columns: {e}")

    def create_csv_download_file(self, df, filename="data.csv"):
        try:
            if isinstance(df, str):
                # df is a file path, read from the file
                csv_str = open(df, 'r').read()
            else:
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

    def create_excel_download_file(self, df, filename="data.xlsx"):
        try:
            if isinstance(df, str):
                # df is a file path, read from the file
                df_copy = pd.read_csv(df)
            else:
                df_copy = df.copy()

            # If df has datetime columns, convert them to timezone-naive
            for col in df_copy.columns:
                if isinstance(df_copy[col].dtype, pd.DatetimeTZDtype):
                    df_copy[col] = df_copy[col].dt.tz_convert(None)

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

    def calculate_correlation(self, time_series: pd.DataFrame, correlations):
        """
        Calculate the Pearson Correlation Coefficient between two time series.

        Parameters:
        time_series: A pandas DataFrame that contains a column 'value'.
        correlations: A tuple of two elements, each representing the metric to be used for correlation calculation.

        Returns:
        A dictionary containing the Pearson Correlation Coefficient and p-value, or None if calculation was not possible.
        """
        metric_one, metric_two = correlations

        ts_metric_one = time_series[time_series['event'] == metric_one]
        ts_metric_one = ts_metric_one[~ts_metric_one.index.duplicated(keep='first')]

        ts_metric_two = time_series[time_series['event'] == metric_two]
        ts_metric_two = ts_metric_two[~ts_metric_two.index.duplicated(keep='first')]

        # Check for sufficient data
        insufficient_data = []
        if len(ts_metric_one) < 2:
            insufficient_data.append(metric_one)
        if len(ts_metric_two) < 2:
            insufficient_data.append(metric_two)

        if insufficient_data:
            print(f"Insufficient data for {', '.join(insufficient_data)}")
            return None

        # Find common timestamps using index intersection
        common_timestamps = ts_metric_one.index.intersection(ts_metric_two.index)

        metric_one_values = ts_metric_one.loc[common_timestamps, 'value'].values
        metric_two_values = ts_metric_two.loc[common_timestamps, 'value'].values

        # Check for same length and at least 2 data points
        if len(metric_one_values) != len(metric_two_values):
            print(f'The two metrics do not have the same amount of sampling in the data.')
            return None
        elif len(metric_one_values) < 2 or len(metric_two_values) < 2:
            print('Both time series need to have at least 2 data points to calculate correlation.')
            return None

        correlation, p_val = pearsonr(metric_one_values, metric_two_values)

        return {"Correlation": correlation, "P-value": p_val, "Metric One": metric_one, "Metric Two": metric_two}

    def parse_host_data_query(self, host_sql):
        """
        Parses the provided SQL query tuple to identify matched keywords based on the provided mapped units dictionary.
        The function attempts to find matches between the query parameters and the keys or values of the mapped_units
        dictionary.

        Returns:
        :return: A list of strings containing the matched keywords from the mapped_units dictionary based on the SQL
                 query's parameters. If no matches are found or an error occurs, it returns all the keys from the
                 mapped_units dictionary.
        """
        mapped_units = {
            "CPU %": "cpuuser",
            "GPU %": "gpu_usage",
            "GB:memused": "memused",
            "GB:memused_minus_diskcache": "memused_minus_diskcache",
            "GB/s": "block",
            "MB/s": "nfs"
        }
        if not isinstance(host_sql, tuple) or len(host_sql) < 2:
            return list(mapped_units.keys())

        _, params = host_sql

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
