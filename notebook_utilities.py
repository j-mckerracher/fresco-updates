import io
import os
import matplotlib
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
from IPython.display import display, clear_output, HTML
import ipywidgets as widgets


class NotebookUtilities:
    def __init__(self):
        self.where_conditions_values = []
        self.where_conditions_jobs = []
        self.time_window_valid_jobs = False
        self.MAX_DAYS_HOSTS = 31
        self.MAX_DAYS_JOBS = 180
        self.account_log_df = pd.DataFrame()
        self.host_data_sql_query = ""
        self.where_conditions_hosts = []
        self.time_window_valid_hosts = False
        self.time_series_df = pd.DataFrame()
        self.value_input_hosts = ""
        self.error_output_hosts = widgets.Output()
        self.job_data_columns_dropdown = widgets.SelectMultiple()
        self.validate_button_jobs = widgets.Button()
        self.start_time_jobs = widgets.NaiveDatetimePicker()
        self.end_time_jobs = widgets.NaiveDatetimePicker()
        self.output_jobs = widgets.Output()
        self.query_output_jobs = widgets.Output()
        self.error_output_jobs = widgets.Output()
        self.columns_dropdown_jobs = widgets.Dropdown()
        self.host_data_columns_dropdown = widgets.SelectMultiple()
        self.operators_dropdown_jobs = widgets.Dropdown()
        self.condition_list_jobs = widgets.SelectMultiple()
        self.validate_button_hosts = widgets.Button()
        self.start_time_hosts = widgets.NaiveDatetimePicker()
        self.end_time_hosts = widgets.NaiveDatetimePicker()
        # self.start_time_hosts = widgets.DatePicker()
        # self.end_time_hosts = widgets.DatePicker()
        self.output_hosts = widgets.Output()
        self.query_output_hosts = widgets.Output()
        self.operators_dropdown_hosts = widgets.Dropdown()
        self.columns_dropdown_hosts = widgets.Dropdown()
        self.condition_list_hosts = widgets.SelectMultiple()
        self.value_input_container_hosts = widgets.HBox()
        self.value_input_container_jobs = widgets.HBox()
        self.stats = widgets.SelectMultiple()
        self.ratio_threshold = widgets.IntText()
        self.interval_type = widgets.Dropdown()
        self.time_units = widgets.Dropdown()
        self.time_value = widgets.IntText()
        self.distinct_checkbox = widgets.Checkbox()
        self.order_by_dropdown = widgets.Dropdown()
        self.order_by_direction_dropdown = widgets.Dropdown()
        self.aggregation_function_dropdown = widgets.Dropdown()
        self.limit_input = widgets.IntText()
        self.in_values_textarea = widgets.Textarea()
        self.in_values_dropdown = widgets.Dropdown()

    def get_time_series_df(self):
        return self.time_series_df

    def display_query_jobs(self):
        """
        Displays the current SQL query for jobs based on the specified conditions, columns, and time window.

        This method constructs the SQL query using the selected conditions, columns, and time window from
        the corresponding widgets. It then displays the constructed query and its parameters.

        Attributes used:
        :attr self.where_conditions_jobs: List of conditions to filter the data.
        :attr self.job_data_columns_dropdown: Dropdown widget to select columns for the query.
        :attr self.validate_button_jobs: Button widget to validate the selected time window.
        :attr self.start_time_jobs: Datetime picker widget to select the start time of the query window.
        :attr self.end_time_jobs: Datetime picker widget to select the end time of the query window.
        :attr self.query_output_jobs: Output widget to display the constructed query and its parameters.

        Returns:
        :return: None. This method outputs the query and parameters to the notebook directly.
        """
        query, params = self.construct_job_data_query(self.where_conditions_jobs,
                                                      self.job_data_columns_dropdown.value,
                                                      self.validate_button_jobs.description,
                                                      self.start_time_jobs.value,
                                                      self.end_time_jobs.value)
        with self.query_output_jobs:
            clear_output(wait=True)
            print(f"Current SQL query:\n{query}\nParameters: {params}")

    def display_query_hosts(self):
        """
        Displays the current SQL query for hosts based on the specified conditions, columns, and time window.

        This method constructs the SQL query using the selected conditions, columns, and time window from
        the corresponding widgets. It then displays the constructed query and updates the class attribute
        with the current query.

        Attributes used:
        :attr self.where_conditions_hosts: List of conditions to filter the data.
        :attr self.host_data_columns_dropdown: Dropdown widget to select columns for the query.
        :attr self.validate_button_hosts: Button widget to validate the selected time window.
        :attr self.start_time_hosts: Datetime picker widget to select the start time of the query window.
        :attr self.end_time_hosts: Datetime picker widget to select the end time of the query window.
        :attr self.query_output_hosts: Output widget to display the constructed query.
        :attr self.host_data_sql_query: Class attribute to store the current SQL query for hosts.

        Returns:
        :return: None. This method outputs the query to the notebook directly and updates the class attribute.
        """
        query, params = self.construct_query_hosts(self.where_conditions_hosts,
                                                   self.host_data_columns_dropdown.value,
                                                   self.validate_button_hosts.description,
                                                   self.start_time_hosts.value,
                                                   self.end_time_hosts.value)
        with self.query_output_hosts:
            clear_output(wait=True)
            print(f"Current SQL query:\n{query, params}")
            self.host_data_sql_query = query, params

    def update_value_input_jobs(self, change):
        """
        Updates the input widget based on the changed column selection for job data.

        Depending on the new selected column (from the 'change' dictionary), this method determines
        the appropriate input widget (e.g., a date picker, dropdown, or text box) to be used for
        filtering the data on that column. The input widget is then updated in the `value_input_container_jobs`.

        Parameters:
        :param change: A dictionary containing the 'new' and 'old' values of the changed widget attribute.
                       Expected to come from the `observe` method of an ipywidgets widget.

        Attributes used:
        :attr self.value_input_container_jobs: Container widget holding the current input widget for job data conditions.

        Returns:
        :return: None. This method updates the class attribute directly.
        """
        if '_time' in change['new']:
            value_input = widgets.NaiveDatetimePicker(value=datetime.now().replace(microsecond=0),
                                                      description='Value:')
        elif change['new'] == 'queue':
            value_input = widgets.Dropdown(
                options=['standard', 'wholenode', 'shared', 'highmem', 'gpu', 'benchmarking', 'wide', 'debug',
                         'gpu-debug'],
                description='Value:')
        elif change['new'] == 'exitcode':
            value_input = widgets.Dropdown(options=['TIMEOUT', 'COMPLETED', 'CANCELLED', 'FAILED', 'NODE_FAIL'],
                                           description='Value:')
        else:
            value_input = widgets.Text(description='Value:')
        self.value_input_container_jobs.children = [value_input]


    def add_condition_jobs(self, b):
        """
        Adds a new condition for filtering the job data based on user input.

        This method adds a new filtering condition for the job data based on the selected column, operator,
        and value input by the user. Before adding the condition, it validates the selected time window
        and the condition's value. If the condition is valid, it is added to the `where_conditions_jobs` list,
        and the displayed list of conditions (`condition_list_jobs`) is updated. Finally, the SQL query is
        displayed with the new condition.

        Parameters:
        :param b: The button instance triggering this callback. Not directly used in the method.

        Attributes used:
        :attr self.time_window_valid_jobs: Flag indicating whether the selected time window is valid.
        :attr self.error_output_jobs: Output widget to display error messages.
        :attr self.columns_dropdown_jobs: Dropdown widget to select a column for the condition.
        :attr self.value_input_container_jobs: Container widget holding the current input widget for job data conditions.
        :attr self.operators_dropdown_jobs: Dropdown widget to select an operator for the condition.
        :attr self.where_conditions_jobs: List storing current filtering conditions.
        :attr self.condition_list_jobs: List widget displaying the current filtering conditions.

        Returns:
        :return: None. This method updates class attributes directly and displays messages in the output widget.
        """
        if not self.time_window_valid_jobs:
            with self.error_output_jobs:
                clear_output(wait=True)
                print("Please enter a valid time window before adding conditions.")
            return
        with self.error_output_jobs:
            clear_output(wait=True)
            column = self.columns_dropdown_jobs.value
            value_widget = self.value_input_container_jobs.children[0]
            if isinstance(value_widget, widgets.Dropdown):
                value = value_widget.value
            elif isinstance(value_widget.value, str):
                value = value_widget.value.upper()
            else:
                value = value_widget.value
            error_message = self.validate_condition_jobs(column, value)
            if error_message:
                print(error_message)
            else:
                condition = (column, self.operators_dropdown_jobs.value, value)
                self.where_conditions_jobs.append(condition)
                self.condition_list_jobs.options = [f"{col} {op} '{val}'" for col, op, val in
                                                    self.where_conditions_jobs]
                self.display_query_jobs()

    def add_condition_hosts(self, b):
        """
        Adds a new condition for filtering the host data based on user input.

        This method adds a new filtering condition for the host data based on the selected column, operator,
        and value input by the user. Before adding the condition, it validates the selected time window
        and the condition's value. If the condition is valid, it is added to the `where_conditions_hosts` list,
        and the displayed list of conditions (`condition_list_hosts`) is updated. Finally, the SQL query is
        displayed with the new condition.

        Parameters:
        :param b: The button instance triggering this callback. Not directly used in the method.

        Attributes used:
        :attr self.time_window_valid_hosts: Flag indicating whether the selected time window is valid.
        :attr self.error_output_hosts: Output widget to display error messages.
        :attr self.columns_dropdown_hosts: Dropdown widget to select a column for the condition.
        :attr self.value_input_hosts: Input widget to specify the value for the condition.
        :attr self.operators_dropdown_hosts: Dropdown widget to select an operator for the condition.
        :attr self.where_conditions_hosts: List storing current filtering conditions for hosts.
        :attr self.condition_list_hosts: List widget displaying the current filtering conditions for hosts.

        Returns:
        :return: None. This method updates class attributes directly and displays messages in the output widget.
        """
        if not self.time_window_valid_hosts:
            with self.error_output_hosts:
                clear_output(wait=True)
                print("Please enter a valid time window before adding conditions.")
            return
        with self.error_output_hosts:
            clear_output(wait=True)
            column = self.columns_dropdown_hosts.value
            value = self.value_input_hosts.value
            if 'job' in value.casefold() or 'node' in value.casefold():
                value = value.upper()

            error_message = self.validate_condition_hosts(column, value)
            if error_message:
                print(error_message)
            else:
                # Use %s as a placeholder for the actual value
                condition = (column, self.operators_dropdown_hosts.value, "%s")
                self.where_conditions_hosts.append(condition)

                # Store the actual value separately
                self.where_conditions_values.append(value)

                self.condition_list_hosts.options = [f"{col} {op} '{val}'" for col, op, val in
                                                     self.where_conditions_hosts]
                self.display_query_hosts()

    def remove_condition_jobs(self, b):
        """
        Removes specified conditions from the filtering criteria for job data.

        This method removes the selected conditions from the `where_conditions_jobs` list based on the user's
        selection in the `condition_list_jobs` widget. After removal, it updates the displayed list of conditions
        (`condition_list_jobs`) and displays the updated SQL query.

        Parameters:
        :param b: The button instance triggering this callback. Not directly used in the method.

        Attributes used:
        :attr self.error_output_jobs: Output widget to display error messages or notifications.
        :attr self.condition_list_jobs: List widget displaying the current filtering conditions for jobs.
        :attr self.where_conditions_jobs: List storing current filtering conditions for jobs.

        Returns:
        :return: None. This method updates class attributes directly and displays the SQL query in the output widget.
        """
        with self.error_output_jobs:
            clear_output(wait=True)
            for condition in self.condition_list_jobs.value:
                index = self.condition_list_jobs.options.index(condition)
                self.where_conditions_jobs.pop(index)
            self.condition_list_jobs.options = [f"{col} {op} '{val}'" for col, op, val in
                                                self.where_conditions_jobs]
            self.display_query_jobs()

    def remove_condition_hosts(self, b):
        """
        Removes specified conditions from the filtering criteria for host data.

        This method removes the selected conditions from the `where_conditions_hosts` list based on the user's
        selection in the `condition_list_hosts` widget. After removal, it updates the displayed list of conditions
        (`condition_list_hosts`) and displays the updated SQL query.

        Parameters:
        :param b: The button instance triggering this callback. Not directly used in the method.

        Attributes used:
        :attr self.error_output_hosts: Output widget to display error messages or notifications.
        :attr self.condition_list_hosts: List widget displaying the current filtering conditions for hosts.
        :attr self.where_conditions_hosts: List storing current filtering conditions for hosts.

        Returns:
        :return: None. This method updates class attributes directly and displays the SQL query in the output widget.
        """
        with self.error_output_hosts:
            clear_output(wait=True)
            selected_conditions = list(self.condition_list_hosts.value)
            for condition in selected_conditions:
                index = self.condition_list_hosts.options.index(condition)
                self.where_conditions_hosts.pop(index)
            self.condition_list_hosts.options = [f"{col} {op} '{val}'" for col, op, val in
                                                 self.where_conditions_hosts]
            self.display_query_hosts()

    def on_button_clicked_jobs(self, b):
        """
        Validates the selected time window for querying job data and displays the corresponding SQL query.

        This method checks the validity of the selected start and end times for querying job data. It updates
        the description and style of the validation button based on the validity of the time window. If the
        selected time window is valid, it clears any error messages and displays the SQL query for the selected
        conditions. If the time window is invalid, it updates the button style and description to notify the user.

        Parameters:
        :param b: The button instance triggering this callback. Used to update the button's description and style based on the validity of the time window.

        Attributes used:
        :attr self.end_time_jobs: Datetime picker widget to select the end time for the query.
        :attr self.start_time_jobs: Datetime picker widget to select the start time for the query.
        :attr self.MAX_DAYS_JOBS: Constant specifying the maximum allowed days for the time window.
        :attr self.time_window_valid_jobs: Boolean indicating if the selected time window is valid.
        :attr self.error_output_jobs: Output widget to display error messages or notifications.

        Returns:
        :return: None. This method updates class attributes directly and displays the SQL query in the output widget.
        """
        print("")
        time_difference = self.end_time_jobs.value - self.start_time_jobs.value
        if self.end_time_jobs.value and self.start_time_jobs.value >= self.end_time_jobs.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.time_window_valid_jobs = False
        elif self.start_time_jobs.value and self.end_time_jobs.value <= self.start_time_jobs.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.time_window_valid_jobs = False
        elif time_difference.days > self.MAX_DAYS_JOBS:
            b.description = "Time Window Too Large"
            b.button_style = 'danger'
            self.time_window_valid_jobs = False
        else:
            b.description = "Times Valid"
            b.button_style = 'success'
            self.time_window_valid_jobs = True
            with self.error_output_jobs:  # Clear the error message if the time window is valid
                clear_output(wait=True)
        self.display_query_jobs()

    def on_button_clicked_hosts(self, b):
        """
        Validates the selected time window for querying host data and displays the corresponding SQL query.

        This method checks the validity of the selected start and end times for querying host data. It updates
        the description and style of the validation button based on the validity of the time window. If the
        selected time window is valid, it clears any error messages and displays the SQL query for the selected
        conditions. If the time window is invalid, it updates the button style and description to notify the user.

        Parameters:
        :param b: The button instance triggering this callback. Used to update the button's description and style based on the validity of the time window.

        Attributes used:
        :attr self.end_time_hosts: Datetime picker widget to select the end time for the query.
        :attr self.start_time_hosts: Datetime picker widget to select the start time for the query.
        :attr self.MAX_DAYS_HOSTS: Constant specifying the maximum allowed days for the time window.
        :attr self.time_window_valid_hosts: Boolean indicating if the selected time window is valid.
        :attr self.error_output_hosts: Output widget to display error messages or notifications.

        Returns:
        :return: None. This method updates class attributes directly and displays the SQL query in the output widget.
        """
        print("")
        time_difference = self.end_time_hosts.value - self.start_time_hosts.value
        if self.end_time_hosts.value and self.start_time_hosts.value >= self.end_time_hosts.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.time_window_valid_hosts = False
        elif self.start_time_hosts.value and self.end_time_hosts.value <= self.start_time_hosts.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.time_window_valid_hosts = False
        elif time_difference.days > self.MAX_DAYS_HOSTS:  # Check if the time window is greater than one month
            b.description = "Time Window Too Large"
            b.button_style = 'danger'
            self.time_window_valid_hosts = False
        else:
            b.description = "Times Valid"
            b.button_style = 'success'
            self.time_window_valid_hosts = True
            with self.error_output_hosts:  # Clear the error message if the time window is valid
                clear_output(wait=True)
        self.display_query_hosts()  # Display the current SQL query for hosts

    def on_execute_button_clicked_jobs(self, b):
        """
        Executes the constructed SQL query for job data, displays the results, and provides options for downloading the data.

        This method validates the selected time window and, if valid, constructs and executes the SQL query for job data
        using the selected conditions. The results are displayed in the output widget. Additionally, the user is given
        the option to download the results as a CSV or Excel file.

        Parameters:
        :param b: The button instance triggering this callback.

        Attributes used:
        :attr self.time_window_valid_jobs: Boolean indicating if the selected time window is valid.
        :attr self.output_jobs: Output widget to display the query results or notifications.
        :attr self.where_conditions_jobs: List of conditions selected for the SQL query.
        :attr self.job_data_columns_dropdown: Dropdown widget for selecting columns to include in the query.
        :attr self.validate_button_jobs: Button widget for time window validation.
        :attr self.start_time_jobs: Datetime picker widget to select the start time for the query.
        :attr self.end_time_jobs: Datetime picker widget to select the end time for the query.
        :attr self.account_log_df: DataFrame to store the results of the executed SQL query.

        Returns:
        :return: None. This method updates class attributes directly and displays the query results or notifications in the output widget.
        """
        with self.output_jobs:
            clear_output(wait=True)  # Clear the previous output
            if not self.time_window_valid_jobs:
                print("Please enter a valid time window before executing the query.")
                return
            try:
                query, params = self.construct_job_data_query(self.where_conditions_jobs,
                                                              self.job_data_columns_dropdown.value,
                                                              self.validate_button_jobs.description,
                                                              self.start_time_jobs.value,
                                                              self.end_time_jobs.value)
                self.account_log_df = self.execute_sql_query_chunked(query, self.account_log_df, params=params)
                print(f"\nResults for query: \n{query}\nParameters: {params}")
                display(self.account_log_df)

                # Code to give user the option to download the filtered data
                print("\nDownload the Job table data? The files will appear on the left in the file explorer.")
                csv_acc_download_button = widgets.Button(description="Download as CSV")
                excel_acc_download_button = widgets.Button(description="Download as Excel")

                start_jobs = self.start_time_jobs.value.strftime('%Y-%m-%d-%H-%M-%S')
                end_jobs = self.end_time_jobs.value.strftime('%Y-%m-%d-%H-%M-%S')

                def on_acc_csv_button_clicked(b):
                    self.create_csv_download_file(self.account_log_df,
                                                  filename=f"job-data-csv-{start_jobs}-to-{end_jobs}.csv")

                def on_acc_excel_button_clicked(b):
                    self.create_excel_download_file(self.account_log_df,
                                                    filename=f"job-data-excel-{start_jobs}-to-{end_jobs}.xlsx")

                csv_acc_download_button.on_click(on_acc_csv_button_clicked)
                excel_acc_download_button.on_click(on_acc_excel_button_clicked)

                # Put the buttons in a horizontal box
                button_box2 = widgets.HBox([csv_acc_download_button, excel_acc_download_button])
                display(button_box2)

            except Exception as e:
                print(f"An error occurred: {e}")

    def on_execute_button_clicked_hosts(self, b):
        """
        Executes the constructed SQL query for host data, displays the results, and provides options for downloading the data.

        This method validates the selected time window and, if valid, constructs and executes the SQL query for host data
        using the selected conditions. The results are displayed in the output widget. Additionally, the user is given
        the option to download the results as a CSV or Excel file.

        Parameters:
        :param b: The button instance triggering this callback.

        Attributes used:
        :attr self.time_window_valid_hosts: Boolean indicating if the selected time window is valid.
        :attr self.output_hosts: Output widget to display the query results or notifications.
        :attr self.where_conditions_hosts: List of conditions selected for the SQL query.
        :attr self.host_data_columns_dropdown: Dropdown widget for selecting columns to include in the query.
        :attr self.validate_button_hosts: Button widget for time window validation.
        :attr self.start_time_hosts: Datetime picker widget to select the start time for the query.
        :attr self.end_time_hosts: Datetime picker widget to select the end time for the query.
        :attr self.time_series_df: DataFrame to store the results of the executed SQL query.
        :attr self.host_data_sql_query: String to store the constructed SQL query.

        Returns:
        :return: None. This method updates class attributes directly and displays the query results or notifications in the output widget.
        """
        with self.output_hosts:
            clear_output(wait=True)  # Clear the previous output
            if not self.time_window_valid_hosts:
                print("Please enter a valid time window before executing the query.")
                return
            try:
                query, params = self.construct_query_hosts(self.where_conditions_hosts,
                                                           self.host_data_columns_dropdown.value,
                                                           self.validate_button_hosts.description,
                                                           self.start_time_hosts.value,
                                                           self.end_time_hosts.value)
                self.time_series_df = self.execute_sql_query_chunked(query, self.time_series_df, params=params)
                print(f"\nResults for query: \n{self.host_data_sql_query}\nParameters: {params}")
                display(self.time_series_df)

                # Code to give user the option to download the filtered data
                print(
                    "\nDownload the filtered Host table data? The files will appear on the left in the file "
                    "explorer.")
                csv_download_button = widgets.Button(description="Download as CSV")
                excel_download_button = widgets.Button(description="Download as Excel")

                start = self.start_time_hosts.value.strftime('%Y-%m-%d-%H-%M-%S')
                end = self.end_time_hosts.value.strftime('%Y-%m-%d-%H-%M-%S')

                def on_csv_button_clicked(b):
                    self.create_csv_download_file(self.time_series_df,
                                                  filename=f"host-data-csv-{start}-to-{end}.csv")

                def on_excel_button_clicked(b):
                    self.create_excel_download_file(self.time_series_df,
                                                    filename=f"host-data-excel-{start}-to-{end}.xlsx")

                csv_download_button.on_click(on_csv_button_clicked)
                excel_download_button.on_click(on_excel_button_clicked)

                # Put the buttons in a horizontal box
                button_box = widgets.HBox([csv_download_button, excel_download_button])
                display(button_box)

            except Exception as e:
                print(f"An error occurred: {e}")

    def on_order_by_changed(self, change):
        """
        Handles the change in the order by dropdown widget.

        This method updates the order by clause of the SQL query constructed.

        Parameters:
        :param change: The change in the dropdown widget.

        Attributes used:
        :attr self.order_by_dropdown: Dropdown widget for selecting the order by clause.
        :attr self.host_data_sql_query: String to store the constructed SQL query.

        Returns:
        :return: None. This method updates class attributes directly and displays the query results or notifications in the output widget.
        """
        if change['new'] == 'None':
            self.host_data_sql_query = self.host_data_sql_query.replace("ORDER BY", "")
            self.display_query_hosts()
        else:
            self.host_data_sql_query = self.host_data_sql_query.replace("ORDER BY", f"ORDER BY {change['new']}")

    def on_distinct_checkbox_change(self, change):
        """
        Callback function to be executed when the value of distinct_checkbox changes.

        Parameters:
        :param change: Contains information about the change.
        """
        if change['name'] == 'value':  # Check if the checkbox is checked
            self.display_query_hosts()

    def on_order_by_dropdown_change(self, change):
        # Check if the change is due to a new value being selected in the dropdown
        if change['type'] == 'change' and change['name'] == 'value':
            # Retrieve the selected column from the dropdown's new value
            order_by_column = change['new']

            # If the dropdown has a 'None' or similar option to indicate no ordering, handle it
            if order_by_column in (None, 'None', ''):
                order_by_column = None

            # Display the updated SQL query
            self.display_query_hosts()

    def on_group_by_changed(self, change):
        if change['new'] != 'None':
            self.display_query_hosts()

    def on_limit_changed(self, change):
        self.display_query_hosts()

    def on_in_values_changed(self, change):
        # Split the string by commas and strip whitespace
        values = [val.strip() for val in change['new'].split(',')]
        # You might want to check if values are valid for the selected column here
        self.display_query_hosts()

    def update_value_input_hosts(self, change):
        """
        Executes the constructed SQL query for host data, displays the results, and provides options for downloading the data.

        This method validates the selected time window and, if valid, constructs and executes the SQL query for host data
        using the selected conditions. The results are displayed in the output widget. Additionally, the user is given
        the option to download the results as a CSV or Excel file.

        Parameters:
        :param b: The button instance triggering this callback.

        Attributes used:
        :attr self.time_window_valid_hosts: Boolean indicating if the selected time window is valid.
        :attr self.output_hosts: Output widget to display the query results or notifications.
        :attr self.where_conditions_hosts: List of conditions selected for the SQL query.
        :attr self.host_data_columns_dropdown: Dropdown widget for selecting columns to include in the query.
        :attr self.validate_button_hosts: Button widget for time window validation.
        :attr self.start_time_hosts: Datetime picker widget to select the start time for the query.
        :attr self.end_time_hosts: Datetime picker widget to select the end time for the query.
        :attr self.time_series_df: DataFrame to store the results of the executed SQL query.
        :attr self.host_data_sql_query: String to store the constructed SQL query.

        Returns:
        :return: None. This method updates class attributes directly and displays the query results or notifications in the output widget.
        """
        if change['new'] == 'unit':
            self.value_input_hosts = widgets.Dropdown(
                options=['CPU %', 'GPU %', 'GB:memused', 'GB:memused_minus_diskcache', 'GB/s', 'MB/s'],
                description='Value:')
        elif change['new'] == 'event':
            self.value_input_hosts = widgets.Dropdown(
                options=['cpuuser', 'block', 'memused', 'memused_minus_diskcache', 'gpu_usage', 'nfs'],
                description='Value:')
        else:
            self.value_input_hosts = widgets.Text(description='Value:')
        self.value_input_container_hosts.children = [self.value_input_hosts]

    def display_widgets(self):
        """
        Displays the widgets for querying both the Host Data Table and the Job Data Table.

        This method initializes and displays a range of user interface widgets allowing users to:
        1. Select columns and time intervals for their queries.
        2. Add and remove conditions to filter the data.
        3. Validate and execute the constructed queries.

        The interface presents two sections side-by-side. The left section is for querying the Host Data Table,
        and the right section is for querying the Job Data Table.

        Parameters:
        None.

        Returns:
        :return: None. This method displays the widgets directly in the UI.
        """
        # ****************************** HOST DATA WIDGETS **********************************

        # Widgets for host data
        banner_hosts_message = widgets.HTML("<h1>Query the Host Data Table</h1>")
        query_time_message_hosts = widgets.HTML(
            f"<h5>Please select the start and end times for your query. Max of <b>{self.MAX_DAYS_HOSTS}</b> days per query.</h5>")
        query_cols_message = widgets.HTML("<h5>Please select columns you want to query:</h5>")
        request_filters_message = widgets.HTML("<h5>Please add conditions to filter the data:</h5>")
        current_filters_message = widgets.HTML("<h5>Current filtering conditions:</h5>")
        self.host_data_columns_dropdown = widgets.SelectMultiple(
            options=['*', 'host', 'jid', 'type', 'event', 'unit', 'value', 'diff', 'arc'], value=['*'],
            description='Columns:')
        self.columns_dropdown_hosts = widgets.Dropdown(
            options=['host', 'jid', 'type', 'event', 'unit', 'value', 'diff', 'arc'],
            description='Column:')
        self.operators_dropdown_hosts = widgets.Dropdown(options=['=', '!=', '<', '>', '<=', '>=', 'LIKE'],
                                                         description='Operator:')
        self.value_input_hosts = widgets.Text(description='Value:')
        # self.start_time_hosts = widgets.DatePicker()
        # self.end_time_hosts = widgets.DatePicker()

        self.start_time_hosts = widgets.NaiveDatetimePicker(value=datetime.now().replace(microsecond=0),
                                                            description='Start Time:')
        self.end_time_hosts = widgets.NaiveDatetimePicker(value=datetime.now().replace(microsecond=0),
                                                          description='End Time:')
        self.validate_button_hosts = widgets.Button(description="Validate Dates")
        self.execute_button_hosts = widgets.Button(description="Execute Query")
        self.add_condition_button_hosts = widgets.Button(description="Add Condition")
        self.remove_condition_button_hosts = widgets.Button(description="Remove Condition")
        self.condition_list_hosts = widgets.SelectMultiple(options=[], description='Conditions:')

        # For DISTINCT
        self.distinct_checkbox = widgets.Checkbox(
            value=False,
            description='Distinct',
            disabled=False
        )

        # For ORDER BY
        self.order_by_dropdown = widgets.Dropdown(
            options=['None'] + [col for col in self.host_data_columns_dropdown.options if col != "*"],
            value='None',
            description='Order By:'
        )
        self.order_by_direction_dropdown = widgets.Dropdown(
            options=['ASC', 'DESC'],
            value='ASC',
            description='Direction:'
        )

        # For LIMIT
        self.limit_input = widgets.IntText(
            value=0,
            description='Limit Results:',
            disabled=False
        )

        # For IN condition
        self.in_values_dropdown = widgets.Dropdown(
            options=['None'] + [col for col in self.host_data_columns_dropdown.options if col != "*"],
            value='None',
            description='IN Column:'
        )

        self.in_values_textarea = widgets.Textarea(
            value='',
            placeholder='Enter values separated by commas',
            description='IN values:',
            disabled=False
        )

        # Attach the update function to the 'columns_dropdown' widget
        self.columns_dropdown_hosts.observe(self.update_value_input_hosts, names='value')
        self.distinct_checkbox.observe(self.on_distinct_checkbox_change)
        self.order_by_dropdown.observe(self.on_order_by_dropdown_change)
        self.order_by_direction_dropdown.observe(self.on_order_by_dropdown_change)
        self.host_data_columns_dropdown.observe(self.on_columns_changed, names='value')
        self.host_data_columns_dropdown.observe(self.update_order_by_options, names='value')
        self.limit_input.observe(self.on_limit_input_change)
        self.in_values_dropdown.observe(self.on_in_values_dropdown_change)
        self.in_values_dropdown.observe(self.update_in_values_options, names='value')
        self.in_values_textarea.observe(self.in_values_text_area_change)

        # Container to hold the value input widget
        self.value_input_container_hosts = widgets.HBox([self.value_input_hosts])

        # Button events.
        self.validate_button_hosts.on_click(self.on_button_clicked_hosts)
        self.execute_button_hosts.on_click(self.on_execute_button_clicked_hosts)
        self.add_condition_button_hosts.on_click(self.add_condition_hosts)
        self.remove_condition_button_hosts.on_click(self.remove_condition_hosts)

        condition_buttons = widgets.HBox([self.add_condition_button_hosts, self.remove_condition_button_hosts])

        # Group the widgets for "hosts" into a VBox
        hosts_group = widgets.VBox([
            banner_hosts_message,
            query_time_message_hosts,
            self.start_time_hosts,
            self.end_time_hosts,
            self.validate_button_hosts,
            query_cols_message,
            self.host_data_columns_dropdown,
            self.distinct_checkbox,  # New widget for DISTINCT
            self.order_by_dropdown,  # New widget for ORDER BY
            self.order_by_direction_dropdown,  # New widget for ORDER BY direction
            self.limit_input,  # New widget for LIMIT
            self.in_values_dropdown,
            self.in_values_textarea,  # New widget for IN condition
            request_filters_message,
            self.columns_dropdown_hosts,
            self.operators_dropdown_hosts,
            self.value_input_container_hosts,
            condition_buttons,
            current_filters_message,
            self.condition_list_hosts,
            self.error_output_hosts,
            self.execute_button_hosts,
            self.query_output_hosts,
            self.output_hosts
        ])

        # ****************************** JOB DATA WIDGETS **********************************

        # Widgets for job_data
        banner_jobs = widgets.HTML("<h1>Query the Job Data Table</h1>")
        query_time_message_jobs = widgets.HTML(
            f"<h5>Please select the start and end times for your query. Max of <b>{self.MAX_DAYS_JOBS}</b> days "
            f"per query.</h5>")
        self.job_data_columns_dropdown = widgets.SelectMultiple(
            options=['*', 'jid', 'submit_time', 'start_time', 'end_time', 'runtime', 'timelimit', 'node_hrs',
                     'nhosts',
                     'ncores', 'ngpus', 'username', 'account', 'queue', 'state', 'jobname', 'exitcode',
                     'host_list'],
            value=['*'], description='Columns:')
        self.columns_dropdown_jobs = widgets.Dropdown(
            options=['jid', 'submit_time', 'start_time', 'end_time', 'runtime', 'timelimit', 'node_hrs', 'nhosts',
                     'ncores',
                     'ngpus', 'username', 'account', 'queue', 'state', 'jobname', 'exitcode', 'host_list'],
            description='Column:')
        self.operators_dropdown_jobs = widgets.Dropdown(options=['=', '!=', '<', '>', '<=', '>=', 'LIKE'],
                                                        description='Operator:')
        self.value_input_jobs = widgets.Text(description='Value:')
        self.start_time_jobs = widgets.NaiveDatetimePicker(value=datetime.now().replace(microsecond=0),
                                                           description='Start Time:')
        self.end_time_jobs = widgets.NaiveDatetimePicker(value=datetime.now().replace(microsecond=0),
                                                         description='End Time:')
        self.validate_button_jobs = widgets.Button(description="Validate Dates")
        self.execute_button_jobs = widgets.Button(description="Execute Query")
        self.add_condition_button_jobs = widgets.Button(description="Add Condition")
        self.remove_condition_button_jobs = widgets.Button(description="Remove Condition")
        self.condition_list_jobs = widgets.SelectMultiple(options=[], description='Conditions:')

        # Attach the update function to the 'columns_dropdown' widget
        self.columns_dropdown_jobs.observe(self.update_value_input_jobs, names='value')

        # Container to hold the value input widget
        self.value_input_container_jobs = widgets.HBox([self.value_input_jobs])

        # Button events
        self.validate_button_jobs.on_click(self.on_button_clicked_jobs)
        self.execute_button_jobs.on_click(self.on_execute_button_clicked_jobs)
        self.add_condition_button_jobs.on_click(self.add_condition_jobs)
        self.remove_condition_button_jobs.on_click(self.remove_condition_jobs)
        condition_buttons_jobs = widgets.HBox(
            [self.add_condition_button_jobs, self.remove_condition_button_jobs])  # HBox for the buttons

        # Group the widgets for "jobs" into another VBox
        jobs_group = widgets.VBox([
            banner_jobs,
            query_time_message_jobs,
            self.start_time_jobs,
            self.end_time_jobs,
            self.validate_button_jobs,
            query_cols_message,
            self.job_data_columns_dropdown,
            request_filters_message,
            self.columns_dropdown_jobs,
            self.operators_dropdown_jobs,
            self.value_input_container_jobs,
            condition_buttons_jobs,
            current_filters_message,
            self.condition_list_jobs,
            self.error_output_jobs,
            self.execute_button_jobs,
            self.query_output_jobs,
            self.output_jobs
        ])

        # Use GridBox to place the two VBox widgets side by side
        grid = widgets.GridBox(children=[hosts_group, jobs_group],
                               layout=widgets.Layout(
                                   width='100%',
                                   grid_template_columns='50% 50%',  # Two columns, each taking up 50% of the width
                                   grid_template_rows='auto',  # One row, height determined by content
                               ))

        display(grid)

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

    # Function to execute the SQL query using psycopg2
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
        if self.distinct_checkbox.value:
            query = f"SELECT DISTINCT {selected_columns} FROM {table_name}"
        else:
            query = f"SELECT {selected_columns} FROM {table_name}"

        # Initialize params and local conditions list
        params = []
        local_conditions = where_conditions_hosts.copy()  # Start with the conditions passed in

        # Add the values from where_conditions_values to params
        params.extend(self.where_conditions_values)

        # Handle time validation
        if validate_button_hosts == "Times Valid":
            local_conditions.append(("time", "BETWEEN", "%s AND %s"))
            params.extend([start_time_hosts, end_time_hosts])

        # Handle IN condition
        in_values = [value.strip() for value in self.in_values_textarea.value.split(',')]
        if in_values and in_values[0]:  # Check if the first value is not empty
            in_clause = ', '.join(['%s'] * len(in_values))
            local_conditions.append((self.in_values_dropdown.value, "IN", f"({in_clause})"))
            params.extend(in_values)

        # Construct the WHERE clause
        if local_conditions:
            where_clause = " AND ".join([f"{col} {op} {val}" for col, op, val in local_conditions])
            query += f" WHERE {where_clause}"

        # Handle ORDER BY
        if self.order_by_dropdown.value != 'None':
            query += f" ORDER BY {self.order_by_dropdown.value} {self.order_by_direction_dropdown.value}"

        # Handle LIMIT
        if self.limit_input.value > 0:
            query += f" LIMIT {self.limit_input.value}"

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
                         'nhosts',
                         'ncores', 'ngpus', 'username', 'account', 'queue', 'state', 'jobname', 'exitcode',
                         'host_list',
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

    def display_statistics_widgets(self):
        try:
            self.stats = widgets.SelectMultiple(
                options=['None', 'Mean', 'Median', 'Standard Deviation', 'PDF', 'CDF',
                         'Ratio of Data Outside Threshold'],
                value=['None'],
                description='Statistics',
                disabled=False
            )

            self.ratio_threshold = widgets.IntText(
                value=0,
                description='Value:',
                disabled=True  # disabled by default
            )

            self.interval_type = widgets.Dropdown(
                options=['None', 'Count', 'Time'],
                value='None',
                description='Interval Type',
                disabled=True  # disabled by default
            )

            self.time_units = widgets.Dropdown(
                options=['None', 'Days', 'Hours', 'Minutes', 'Seconds'],
                value='None',
                description='Interval Unit',
                disabled=True  # disabled by default
            )

            self.time_value = widgets.IntText(
                value=0,
                description='Value:',
                disabled=True  # disabled by default
            )

            def on_stats_change(change):
                if change['type'] == 'change' and change['name'] == 'value':
                    if "Ratio of Data Outside Threshold" in change['new']:
                        # enable ratio_threshold if 'Ratio of Data Outside Threshold' is selected
                        self.ratio_threshold.disabled = False
                    else:
                        # disable ratio_threshold if 'Ratio of Data Outside Threshold' is not selected
                        self.ratio_threshold.disabled = True

                    if change['new'][0] != "None":
                        # enable interval_type if stats is not None
                        self.interval_type.disabled = False
                    else:
                        # disable interval_type if stats is None
                        self.interval_type.disabled = True
                        self.interval_type.value = 'None'  # reset interval_type to 'None'

            self.stats.observe(on_stats_change)

            def on_interval_type_change(change):
                if change['type'] == 'change' and change['name'] == 'value':
                    if change['new'] == "None":
                        self.time_units.disabled = True
                        self.time_value.disabled = True
                        self.time_units.value = 'None'  # reset time_units to 'None'
                        self.time_value.value = 0  # reset time_value to 0
                    elif change['new'] == "Time":
                        self.time_units.disabled = False
                        self.time_value.disabled = False
                    elif change['new'] == "Count":
                        self.time_units.disabled = True
                        self.time_value.disabled = False
                    else:
                        self.time_units.disabled = False
                        self.time_value.disabled = False

            self.interval_type.observe(on_interval_type_change)

            # Display the widgets
            print("Please select a statistic to calculate.")
            display(self.stats)
            print("Please provide the threshold if 'Ratio of Data Outside Threshold' was selected.")
            display(self.ratio_threshold)
            print(
                "Please select an interval type to use in the statistic calculation. If count is selected, "
                "the interval will correspond to a count of rows. If time is selected, the interval will be a time "
                "window.")
            display(self.interval_type)
            print("If time was selected, please select the unit of time.")
            display(self.time_units)
            print("Please provide the interval count.")
            display(self.time_value)

            self.remove_columns()
        except NameError:
            print("ERROR: Please make sure to run the previous notebook cell before executing this one.")

    def on_columns_changed(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            self.display_query_hosts()

    def on_host_list_changed(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            self.display_query_jobs()

    def display_plots(self):
        try:
            ts_df = self.get_time_series_df().copy()
            try:
                # Convert the 'time' columns to datetime
                ts_df['time'] = pd.to_datetime(ts_df['time'])
                ts_df = ts_df.set_index('time')
                ts_df = ts_df.sort_index()
            except Exception as e:
                print("")

            metric_func_map = {
                "Mean": self.get_mean if "Mean" in self.stats.value else "",
                "Median": self.get_median if "Median" in self.stats.value else "",
                "Standard Deviation": self.get_standard_deviation if "Standard Deviation" in self.stats.value else "",
                "PDF": self.plot_pdf if "PDF" in self.stats.value else "",
                "CDF": self.plot_cdf if "CDF" in self.stats.value else "",
                "Ratio of Data Outside Threshold": self.plot_data_points_outside_threshold if 'Ratio of Data Outside '
                                                                                              'Threshold' in
                                                                                              self.stats.value else ""
            }

            unit_map = {
                "CPU %": "cpuuser",
                "GPU %": "gpu_usage",
                "GB:memused": "memused",
                "GB:memused_minus_diskcache": "memused_minus_diskcache",
                "GB/s": "block",
                "MB/s": "nfs"
            }

            units = self.parse_host_data_query()  # get units requested in SQL query

            # Initialize the tqdm progress bar
            total_operations = len(units) * len(self.stats.value)
            pbar = tqdm(total=total_operations, desc="Generating chart/s")

            # set up outputs and tabbed layout
            tab = widgets.Tab()
            outputs = {}
            stat_values = []
            basic_stats = ['Mean', 'Median', 'Standard Deviation']

            # Populate the outputs dictionary
            for unit in units:
                outputs[unit] = {}
                if any(stat in self.stats.value for stat in basic_stats):
                    for stat in self.stats.value + ('Box and Whisker',):
                        outputs[unit][stat] = widgets.Output()
                else:
                    for stat in self.stats.value:
                        outputs[unit][stat] = widgets.Output()

            # set the tab children
            if any(stat in self.stats.value for stat in basic_stats):
                tab.children = [widgets.Accordion([widgets.Box([widgets.Label(stat), outputs[unit][stat]]) for stat in
                                                   self.stats.value + ('Box and Whisker',)],
                                                  titles=self.stats.value + ('Box and Whisker',)) for unit in units]
            else:
                tab.children = [
                    widgets.Accordion(
                        [widgets.Box([widgets.Label(stat), outputs[unit][stat]]) for stat in self.stats.value],
                        titles=self.stats.value) for unit in units]

            tab.titles = units

            message_display = widgets.HTML(value="Initializing . . .")
            display(message_display)

            with plt.style.context('fivethirtyeight'):
                unit_stat_dfs = {}
                time_map = {'Days': 'D', 'Hours': 'H', 'Minutes': 'T', 'Seconds': 'S'}
                for unit in units:
                    unit_stat_dfs[unit] = {}
                    for metric in self.stats.value:
                        message_display.value = ""
                        message_display.value = f"Generating plot for unit <b>{unit}</b> with metric <b>{metric}</b>..."
                        metric_df = ts_df.query(f"`event` == '{unit_map[unit]}'")
                        rolling = False

                        # Calculate stats
                        if self.interval_type.value == "Time":
                            rolling = True
                            try:
                                window = f"{self.time_value.value}{time_map[self.time_units.value]}"
                            except KeyError:
                                print("Error! Please ensure a selection was made in the 'Interval Unit' dropdown.")
                        elif self.interval_type.value == "Count":
                            rolling = True
                            window = self.time_value.value

                        # Handle special cases outside the rolling condition
                        if metric == "PDF":
                            with outputs[unit][metric]:
                                unit_stat_dfs[unit][metric] = metric_func_map[metric](metric_df)
                            continue
                        elif metric == "CDF":
                            with outputs[unit][metric]:
                                unit_stat_dfs[unit][metric] = metric_func_map[metric](metric_df)
                            continue
                        elif metric == "Ratio of Data Outside Threshold":
                            with outputs[unit][metric]:
                                unit_stat_dfs[unit][metric] = metric_func_map[metric](self.ratio_threshold.value,
                                                                                      metric_df)
                            continue

                        # Update the progress bar
                        pbar.update(1)

                        # Only calculate and plot basic stats if rolling is True
                        if rolling:
                            unit_stat_dfs[unit][metric] = metric_func_map[metric](metric_df, rolling=True,
                                                                                  window=window)

                            # Plot stats
                            with outputs[unit][metric]:
                                unit_stat_dfs[unit][metric].plot()
                                x_axis_label = ""
                                if self.interval_type.value == "Count":
                                    x_axis_label += f"Count - Rolling Window: {self.time_value.value} Rows"
                                elif self.interval_type.value == "Time":
                                    x_axis_label += f"Timestamp - Rolling Window: {self.time_value.value}{time_map[self.time_units.value]}"
                                y_axis_label = unit
                                plt.gcf().autofmt_xdate()  # auto formats datetimes
                                plt.style.use('fivethirtyeight')
                                plt.title(f"{unit} {metric}")
                                plt.legend(loc='upper left', fontsize="10")
                                plt.xlabel(x_axis_label)
                                plt.ylabel(y_axis_label)
                                plt.show()

                    # Get the stats dataframes
                    df_mean = unit_stat_dfs[unit].get('Mean')
                    df_std = unit_stat_dfs[unit].get('Standard Deviation')
                    df_median = unit_stat_dfs[unit].get('Median')
                    # Plot box and whisker
                    if any(df is not None for df in [df_mean, df_std, df_median]):
                        with outputs[unit]['Box and Whisker']:
                            self.plot_box_and_whisker(df_mean, df_std, df_median)

                message_display.value = "Plotting complete"
                display(tab)
                pbar.close()
        except NameError as e:
            print("ERROR: Please make sure to run the previous notebook cells before executing this one.")

    def pearson_correlation(self):
        try:
            def on_selection_change(change):
                if len(change.new) > 2:
                    correlations.value = change.new[:2]

            def on_button_click(button):
                graph_output.clear_output()
                with graph_output:
                    with plt.style.context('fivethirtyeight'):
                        display(self.calculate_and_plot_correlation(self.time_series_df, correlations.value))

            correlations = widgets.SelectMultiple(
                options=['None', 'cpuuser', 'gpu_usage', 'nfs', 'block', 'memused', 'memused_minus_diskcache'],
                value=['None'],
                description='Metrics',
                disabled=False
            )

            plot_button = widgets.Button(
                description="Plot correlation",
                disabled=False,
                icon="chart-line"
            )
            plot_button.on_click(on_button_click)

            graph_output = widgets.Output()

            container = widgets.VBox(
                [widgets.HBox([correlations, plot_button], layout=widgets.Layout(
                    width="50%",
                    justify_content="space-between",
                    align_items="center"), ),
                 graph_output])
            correlations.observe(on_selection_change, names='value')

            # Give the user the option to calculate correlations
            print("Please select two metrics below to find their Pearson correlation:")
            display(container)

        except NameError:
            print("ERROR: Please make sure to run the previous notebook cells before executing this one.")

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

    def plot_pdf(self, ts_df: pd.DataFrame):
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

    def plot_cdf(self, ts_df: pd.DataFrame):
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

    def plot_data_points_outside_threshold(self, ratio_threshold_value, ts_df: pd.DataFrame):
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

    def parse_host_data_query(self):
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
        if not isinstance(self.host_data_sql_query, tuple) or len(self.host_data_sql_query) < 2:
            return list(mapped_units.keys())

        _, params = self.host_data_sql_query

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

    def plot_box_and_whisker(self, df_mean: pd.DataFrame, df_std: pd.DataFrame, df_median: pd.DataFrame):
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

    def calculate_and_plot_correlation(self, time_series: pd.DataFrame, correlations):
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

    def create_csv_download_file(self, df, filename="data.csv"):
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

    def create_excel_download_file(self, df, filename="data.xlsx"):
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

    def remove_columns(self):
        """
           Removes specific columns ('type', 'diff', 'arc') from the given dataframe.

           Parameters:
           :param df: A pandas DataFrame

           Returns:
           :return: A pandas DataFrame with the specified columns removed.
           """
        if not isinstance(self.time_series_df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame.")

        columns_to_remove = ['type', 'diff', 'arc']

        try:
            self.time_series_df.drop(columns=columns_to_remove, errors='ignore', inplace=True)
        except Exception as e:
            raise RuntimeError(f"An error occurred while removing columns: {e}")

    def on_limit_input_change(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            if self.limit_input.value > 0:
                self.display_query_hosts()

    def update_order_by_options(self, change):
        # If * is selected, set all available columns as options
        if '*' in self.host_data_columns_dropdown.value:
            all_columns = ['host', 'jid', 'type', 'event', 'unit', 'value', 'diff', 'arc']
            self.order_by_dropdown.options = all_columns
        else:
            # Set the selected columns as options for order_by_dropdown
            self.order_by_dropdown.options = self.host_data_columns_dropdown.value

    def in_values_text_area_change(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            self.display_query_hosts()

    def on_in_values_dropdown_change(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            self.display_query_hosts()

    def update_in_values_options(self, change):
        # If * is selected, set all available columns as options
        if '*' in self.host_data_columns_dropdown.value:
            all_columns = ['host', 'jid', 'type', 'event', 'unit', 'value', 'diff', 'arc']
            self.in_values_dropdown.options = all_columns
        else:
            # Set the selected columns as options for order_by_dropdown
            self.in_values_dropdown.options = self.host_data_columns_dropdown.value
