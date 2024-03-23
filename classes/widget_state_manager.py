from IPython.display import display, clear_output, HTML
from classes.database_manager import DatabaseManager
from classes.data_processor import DataProcessor
from ipywidgets import widgets
from datetime import datetime
import pandas as pd


class WidgetStateManager:
    def __init__(self, base_widget_manager):
        self.base_widget_manager = base_widget_manager
        self.data_processor = DataProcessor(base_widget_manager)
        self.db_service = DatabaseManager()
        self.attach_host_data_observers()
        self.attach_job_data_observers()

    def attach_host_data_observers(self):
        # Observers
        self.base_widget_manager.columns_dropdown_hosts.observe(self.observer_columns_dropdown_hosts, names='value')
        self.base_widget_manager.distinct_checkbox.observe(self.observer_distinct_checkbox)
        self.base_widget_manager.order_by_dropdown.observe(self.observer_order_by_dropdown)
        self.base_widget_manager.order_by_direction_dropdown.observe(self.observer_order_by_dropdown)
        self.base_widget_manager.host_data_columns_dropdown.observe(self.observer_host_data_columns_dropdown,
                                                                    names='value')
        self.base_widget_manager.limit_input.observe(self.observe_limit_input)
        self.base_widget_manager.in_values_dropdown.observe(self.observe_in_values_dropdown)
        self.base_widget_manager.in_values_textarea.observe(self.observe_in_values_textarea)
        # Button events
        self.base_widget_manager.validate_button_hosts.on_click(self.on_button_clicked_hosts)
        self.base_widget_manager.execute_button_hosts.on_click(self.on_execute_button_clicked_hosts)
        self.base_widget_manager.add_condition_button_hosts.on_click(self.on_add_condition_button_hosts_clicked)
        self.base_widget_manager.remove_condition_button_hosts.on_click(self.on_remove_condition_button_hosts_clicked)

    def attach_job_data_observers(self):
        # Observers
        self.base_widget_manager.data_filtering_cols_dropdown_jobs.observe(
            self.observer_data_filtering_cols_dropdown_jobs, names='value'
        )
        self.base_widget_manager.job_data_columns_dropdown.observe(
            self.observer_job_data_columns_dropdown, names='value'
        )
        self.base_widget_manager.distinct_checkbox_jobs.observe(self.on_distinct_hosts_checkbox_change)
        self.base_widget_manager.order_by_dropdown_jobs.observe(self.observer_order_by_dropdowns)
        self.base_widget_manager.order_by_direction_dropdown_jobs.observe(self.observer_order_by_dropdowns)
        self.base_widget_manager.limit_input_jobs.observe(self.observer_limit_input_jobs)
        self.base_widget_manager.in_values_dropdown_jobs.observe(self.observer_in_values_jobs)
        self.base_widget_manager.in_values_textarea_jobs.observe(self.observer_in_values_jobs)
        # Button events
        self.base_widget_manager.validate_button_jobs.on_click(self.on_button_clicked_jobs)
        self.base_widget_manager.execute_button_jobs.on_click(self.on_execute_button_clicked_jobs)
        self.base_widget_manager.add_condition_button_jobs.on_click(self.add_condition_jobs)
        self.base_widget_manager.remove_condition_button_jobs.on_click(self.remove_condition_jobs)

    def observer_columns_dropdown_hosts(self, change):
        if change['new'] == 'unit':
            self.base_widget_manager.value_input_hosts = widgets.Dropdown(
                options=['CPU %', 'GPU %', 'GB:memused', 'GB:memused_minus_diskcache', 'GB/s', 'MB/s'],
                description='Value:')
        elif change['new'] == 'event':
            self.base_widget_manager.value_input_hosts = widgets.Dropdown(
                options=['cpuuser', 'block', 'memused', 'memused_minus_diskcache', 'gpu_usage', 'nfs'],
                description='Value:')
        else:
            self.base_widget_manager.value_input_hosts = widgets.Text(description='Value:')
        self.base_widget_manager.value_input_container_hosts.children = [self.base_widget_manager.value_input_hosts]

    def observer_distinct_checkbox(self, change):
        """
        Callback function to be executed when the value of distinct_checkbox changes.

        Parameters:
        :param change: Contains information about the change.
        """
        if change['name'] == 'value':  # Check if the checkbox is checked
            self.base_widget_manager.display_query_hosts()

    def observer_order_by_dropdowns(self, change):
        self.base_widget_manager.display_query_jobs()

    def observer_order_by_dropdown(self, change):
        # Check if the change is due to a new value being selected in the dropdown
        if change['type'] == 'change' and change['name'] == 'value':
            # Display the updated SQL query
            self.base_widget_manager.display_query_hosts()

    def observer_host_data_columns_dropdown(self, change):
        if '*' in self.base_widget_manager.host_data_columns_dropdown.value:
            all_columns = ['None', 'host', 'jid', 'type', 'event', 'unit', 'value', 'diff', 'arc']
            self.base_widget_manager.in_values_dropdown.options = all_columns
            self.base_widget_manager.order_by_dropdown.options = all_columns
        else:
            # Set the selected columns as options for order_by_dropdown
            options = ['None'] + list(self.base_widget_manager.host_data_columns_dropdown.value)
            self.base_widget_manager.order_by_dropdown.options = options
            self.base_widget_manager.in_values_dropdown.options = options
        self.base_widget_manager.display_query_hosts()

    def observe_limit_input(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            if self.base_widget_manager.limit_input.value > 0:
                self.base_widget_manager.display_query_hosts()

    def observe_in_values_textarea(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            self.base_widget_manager.display_query_hosts()

    def observe_in_values_dropdown(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            val = self.base_widget_manager.in_values_dropdown.value
            if val != 'None':
                self.base_widget_manager.display_query_hosts()

    def observer_limit_input_jobs(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            if self.base_widget_manager.limit_input_jobs.value > 0:
                self.base_widget_manager.display_query_jobs()

    def observer_in_values_jobs(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            self.base_widget_manager.display_query_jobs()

    def on_host_list_changed(self, change):
        if change['name'] == 'value' and change['type'] == 'change':
            self.base_widget_manager.display_query_jobs()

    def observer_data_filtering_cols_dropdown_jobs(self, change):
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
        self.base_widget_manager.value_input_container_jobs.children = [value_input]

    def observer_job_data_columns_dropdown(self, change):
        # If * is selected, set all available columns as options
        if '*' in self.base_widget_manager.job_data_columns_dropdown.value:
            all_job_cols = ['None'] + [col for col in self.base_widget_manager.job_data_columns_dropdown.options if
                                       col != "*"]
            self.base_widget_manager.order_by_dropdown_jobs.options = all_job_cols
            self.base_widget_manager.in_values_dropdown_jobs.options = all_job_cols
        else:
            # Set the selected columns as options for order_by_dropdown
            options = ['None'] + list(self.base_widget_manager.job_data_columns_dropdown.value)
            self.base_widget_manager.order_by_dropdown_jobs.options = options
            self.base_widget_manager.in_values_dropdown_jobs.options = options
        self.base_widget_manager.display_query_jobs()

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
        if not self.base_widget_manager.time_window_valid_jobs:
            with self.base_widget_manager.error_output_jobs:
                clear_output(wait=True)
                print("Please enter a valid time window before adding conditions.")
            return
        with self.base_widget_manager.error_output_jobs:
            clear_output(wait=True)
            column = self.base_widget_manager.data_filtering_cols_dropdown_jobs.value
            value_widget = self.base_widget_manager.value_input_container_jobs.children[0]
            if isinstance(value_widget, widgets.Dropdown):
                value = value_widget.value
            elif isinstance(value_widget.value, str):
                value = value_widget.value.upper()
            else:
                value = value_widget.value
            error_message = self.base_widget_manager.data_processor.validate_condition_jobs(column, value)
            if error_message:
                print(error_message)
            else:
                condition = (column, self.base_widget_manager.operators_dropdown_jobs.value, value)
                self.base_widget_manager.where_conditions_jobs.append(condition)
                self.base_widget_manager.condition_list_jobs.options = [f"{col} {op} '{val}'" for col, op, val in
                                                                        self.base_widget_manager.where_conditions_jobs]
                self.base_widget_manager.display_query_jobs()

    def on_add_condition_button_hosts_clicked(self, b):
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
        if not self.base_widget_manager.time_window_valid_hosts:
            with self.base_widget_manager.error_output_hosts:
                clear_output(wait=True)
                print("Please enter a valid time window before adding conditions.")
            return
        with self.base_widget_manager.error_output_hosts:
            clear_output(wait=True)
            column = self.base_widget_manager.columns_dropdown_hosts.value
            value = self.base_widget_manager.value_input_hosts.value
            if 'job' in value.casefold() or 'node' in value.casefold():
                value = value.upper()

            error_message = self.base_widget_manager.data_processor.validate_condition_hosts(column, value)
            if error_message:
                print(error_message)
            else:
                # Use %s as a placeholder for the actual value
                condition = (column, self.base_widget_manager.operators_dropdown_hosts.value, "%s")
                self.base_widget_manager.where_conditions_hosts.append(condition)

                # Store the actual value separately
                self.base_widget_manager.where_conditions_values.append(value)

                # When displaying to the user, replace %s with the actual value
                display_conditions = [
                    f"{col} {op} '{val}'" if "%s" not in val else f"{col} {op} '{self.base_widget_manager.where_conditions_values[i]}'"
                    for i, (col, op, val) in enumerate(self.base_widget_manager.where_conditions_hosts)
                ]
                self.base_widget_manager.condition_list_hosts.options = display_conditions
                self.base_widget_manager.display_query_hosts()

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
        with self.base_widget_manager.error_output_jobs:
            clear_output(wait=True)
            for condition in self.base_widget_manager.condition_list_jobs.value:
                index = self.base_widget_manager.condition_list_jobs.options.index(condition)
                self.base_widget_manager.where_conditions_jobs.pop(index)
            self.base_widget_manager.condition_list_jobs.options = [f"{col} {op} '{val}'" for col, op, val in
                                                                    self.base_widget_manager.where_conditions_jobs]
            self.base_widget_manager.display_query_jobs()

    def on_remove_condition_button_hosts_clicked(self, b):
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
        with self.base_widget_manager.error_output_hosts:
            clear_output(wait=True)
            selected_conditions = list(self.base_widget_manager.condition_list_hosts.value)
            for condition in selected_conditions:
                index = self.base_widget_manager.condition_list_hosts.options.index(condition)
                # Remove the corresponding value from where_conditions_values
                self.base_widget_manager.where_conditions_values.pop(index)
                # Remove the condition from where_conditions_hosts
                self.base_widget_manager.where_conditions_hosts.pop(index)
            self.base_widget_manager.condition_list_hosts.options = [f"{col} {op} '{val}'" for col, op, val in
                                                                     self.base_widget_manager.where_conditions_hosts]
            self.base_widget_manager.display_query_hosts()

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
        time_difference = self.base_widget_manager.end_time_jobs.value - self.base_widget_manager.start_time_jobs.value
        if self.base_widget_manager.end_time_jobs.value and self.base_widget_manager.start_time_jobs.value >= self.base_widget_manager.end_time_jobs.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.base_widget_manager.time_window_valid_jobs = False
        elif self.base_widget_manager.start_time_jobs.value and self.base_widget_manager.end_time_jobs.value <= self.base_widget_manager.start_time_jobs.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.base_widget_manager.time_window_valid_jobs = False
        elif time_difference.days > self.base_widget_manager.MAX_DAYS_JOBS:
            b.description = "Time Window Too Large"
            b.button_style = 'danger'
            self.base_widget_manager.time_window_valid_jobs = False
        else:
            b.description = "Times Valid"
            b.button_style = 'success'
            self.base_widget_manager.time_window_valid_jobs = True
            with self.base_widget_manager.error_output_jobs:  # Clear the error message if the time window is valid
                clear_output(wait=True)
        self.base_widget_manager.display_query_jobs()

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
        time_difference = self.base_widget_manager.end_time_hosts.value - self.base_widget_manager.start_time_hosts.value
        if self.base_widget_manager.end_time_hosts.value and self.base_widget_manager.start_time_hosts.value >= self.base_widget_manager.end_time_hosts.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.base_widget_manager.time_window_valid_hosts = False
        elif self.base_widget_manager.start_time_hosts.value and self.base_widget_manager.end_time_hosts.value <= self.base_widget_manager.start_time_hosts.value:
            b.description = "Invalid Times"
            b.button_style = 'danger'
            self.base_widget_manager.time_window_valid_hosts = False
        elif time_difference.days > self.base_widget_manager.MAX_DAYS_HOSTS:
            b.description = "Time Window Too Large"
            b.button_style = 'danger'
            self.base_widget_manager.time_window_valid_hosts = False
        else:
            b.description = "Times Valid"
            b.button_style = 'success'
            self.base_widget_manager.time_window_valid_hosts = True
            with self.base_widget_manager.error_output_hosts:  # Clear the error message if the time window is valid
                clear_output(wait=True)
        self.base_widget_manager.display_query_hosts()  # Display the current SQL query for hosts

    def on_execute_button_clicked_hosts(self, b):
        with self.base_widget_manager.output_hosts:
            clear_output(wait=True)  # Clear the previous output
            if not self.base_widget_manager.time_window_valid_hosts:
                print("Please enter a valid time window before executing the query.")
                return
            try:
                query, params = self.data_processor.construct_query_hosts(
                    self.base_widget_manager.where_conditions_hosts,
                    self.base_widget_manager.host_data_columns_dropdown.value,
                    self.base_widget_manager.validate_button_hosts.description,
                    self.base_widget_manager.start_time_hosts.value,
                    self.base_widget_manager.end_time_hosts.value)

                # Delete previous query results file if it exists
                self.db_service.delete_query_results_file()

                result = self.db_service.execute_sql_query_chunked(
                    query,
                    self.base_widget_manager.time_series_df,
                    params=params
                )

                if isinstance(result, str):
                    # Result is a file path, data was streamed to disk
                    self.base_widget_manager.time_series_df = pd.read_csv(result)
                    display(self.base_widget_manager.time_series_df.head())
                    print("Results truncated due to memory limitations. Full results saved to disk.")
                else:
                    self.base_widget_manager.time_series_df = result
                    display(self.base_widget_manager.time_series_df)

                # Code to give user the option to download the filtered data
                print(
                    "\nDownload the filtered Host table data? The files will appear on the left in the file explorer.")
                csv_download_button = widgets.Button(description="Download as CSV")
                excel_download_button = widgets.Button(description="Download as Excel")

                start = self.base_widget_manager.start_time_hosts.value.strftime('%Y-%m-%d-%H-%M-%S')
                end = self.base_widget_manager.end_time_hosts.value.strftime('%Y-%m-%d-%H-%M-%S')

                def on_csv_button_clicked(b):
                    self.data_processor.create_csv_download_file(self.base_widget_manager.time_series_df,
                                                                 filename=f"host-data-csv-{start}-to-{end}.csv")

                def on_excel_button_clicked(b):
                    self.data_processor.create_excel_download_file(self.base_widget_manager.time_series_df,
                                                                   filename=f"host-data-excel-{start}-to-{end}.xlsx")

                csv_download_button.on_click(on_csv_button_clicked)
                excel_download_button.on_click(on_excel_button_clicked)

                # Put the buttons in a horizontal box
                button_box = widgets.HBox([csv_download_button, excel_download_button])
                display(button_box)

            except Exception as e:
                print(f"An error occurred: {e}")

    def on_execute_button_clicked_jobs(self, b):
        with self.base_widget_manager.output_jobs:
            clear_output(wait=True)  # Clear the previous output
            if not self.base_widget_manager.time_window_valid_jobs:
                print("Please enter a valid time window before executing the query.")
                return
            try:
                query, params = self.data_processor.construct_job_data_query(
                    self.base_widget_manager.where_conditions_jobs,
                    self.base_widget_manager.job_data_columns_dropdown.value,
                    self.base_widget_manager.validate_button_jobs.description,
                    self.base_widget_manager.start_time_jobs.value,
                    self.base_widget_manager.end_time_jobs.value
                )

                # Delete previous query results file if it exists
                self.db_service.delete_query_results_file()

                result = self.db_service.execute_sql_query_chunked(
                    query,
                    self.base_widget_manager.account_log_df,
                    params=params
                )

                if isinstance(result, str):
                    # Result is a file path, data was streamed to disk
                    self.base_widget_manager.account_log_df = pd.read_csv(result)
                    display(self.base_widget_manager.account_log_df.head())
                    print("Results truncated due to memory limitations. Full results saved to disk.")
                else:
                    self.base_widget_manager.account_log_df = result
                    display(self.base_widget_manager.account_log_df)

                # Code to give user the option to download the filtered data
                print("\nDownload the Job table data? The files will appear on the left in the file explorer.")
                csv_acc_download_button = widgets.Button(description="Download as CSV")
                excel_acc_download_button = widgets.Button(description="Download as Excel")

                start_jobs = self.base_widget_manager.start_time_jobs.value.strftime('%Y-%m-%d-%H-%M-%S')
                end_jobs = self.base_widget_manager.end_time_jobs.value.strftime('%Y-%m-%d-%H-%M-%S')

                def on_acc_csv_button_clicked(b):
                    self.data_processor.create_csv_download_file(self.base_widget_manager.account_log_df,
                                                                 filename=f"job-data-csv-{start_jobs}-to-{end_jobs}.csv")

                def on_acc_excel_button_clicked(b):
                    self.data_processor.create_excel_download_file(self.base_widget_manager.account_log_df,
                                                                   filename=f"job-data-excel-{start_jobs}-to-{end_jobs}.xlsx")

                csv_acc_download_button.on_click(on_acc_csv_button_clicked)
                excel_acc_download_button.on_click(on_acc_excel_button_clicked)

                # Put the buttons in a horizontal box
                button_box2 = widgets.HBox([csv_acc_download_button, excel_acc_download_button])
                display(button_box2)

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
            self.base_widget_manager.host_data_sql_query = self.base_widget_manager.host_data_sql_query.replace(
                "ORDER BY", "")
            self.base_widget_manager.display_query_hosts()
        else:
            self.base_widget_manager.host_data_sql_query = self.base_widget_manager.host_data_sql_query.replace(
                "ORDER BY", f"ORDER BY {change['new']}")

    def on_distinct_hosts_checkbox_change(self, change):
        """
        Callback function to be executed when the value of distinct_hosts_checkbox changes.

        Parameters:
        :param change: Contains information about the change.
        """
        if change['name'] == 'value':  # Check if the checkbox is checked
            self.base_widget_manager.display_query_jobs()

    def on_group_by_changed(self, change):
        if change['new'] != 'None':
            self.base_widget_manager.display_query_hosts()

    def on_limit_changed(self, change):
        self.base_widget_manager.display_query_hosts()

    def on_in_values_changed(self, change):
        # Split the string by commas and strip whitespace
        values = [val.strip() for val in change['new'].split(',')]
        # You might want to check if values are valid for the selected column here
        self.base_widget_manager.display_query_hosts()
