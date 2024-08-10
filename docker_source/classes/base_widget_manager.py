import ipywidgets as widgets
from datetime import datetime
import pandas as pd
from IPython.display import display, clear_output
from matplotlib import pyplot as plt
from classes.display_plots import DisplayPlots
from classes.data_processor import DataProcessor
from classes.database_manager import DatabaseManager
from classes.plotting_manager import PlottingManager


class BaseWidgetManager:
    def __init__(self):
        self.data_processor = DataProcessor(self)
        self.db_service = DatabaseManager()
        self.plotting_service = PlottingManager(self)
        self.init_non_widget_variables()
        self.init_common_widgets()
        self.initialize_host_data_widgets()
        self.initialize_job_data_widgets()
        self.initialize_stats_widgets()
        self.group_and_display_widgets()

    def init_non_widget_variables(self):
        self.where_conditions_values = []
        self.where_conditions_jobs = []
        self.time_window_valid_jobs = False
        self.MAX_DAYS_HOSTS = 5
        self.MAX_DAYS_JOBS = 180
        self.account_log_df = pd.DataFrame()
        self.host_data_sql_query = ""
        self.where_conditions_hosts = []
        self.time_window_valid_hosts = False
        self.time_series_df = pd.DataFrame()

    def init_common_widgets(self):
        self.query_cols_message = widgets.HTML("<h4>Select columns:</h4>")
        self.request_filters_message = widgets.HTML("<h4>Add data filters:</h4>")
        self.current_filters_message = widgets.HTML("<h4>Active filters:</h4>")
        self.order_by_message = widgets.HTML("<h4>Choose sort column and direction:</h4>")
        self.limit_message = widgets.HTML("<h4>Set results limit:</h4>")
        self.in_values_message = widgets.HTML("<h4>Enter IN clause values:</h4>")

    def initialize_host_data_widgets(self):
        self.error_output_hosts = widgets.Output()
        self.output_hosts = widgets.Output()
        self.query_output_hosts = widgets.Output()
        self.banner_hosts_message = widgets.HTML("<h1>Query the Host Data Table</h1>")
        self.query_time_message_hosts = widgets.HTML(
            f"<h4>Select start and end times (Max: <b>{self.MAX_DAYS_HOSTS}</b> days).</h4>")
        self.host_data_columns_dropdown = widgets.SelectMultiple(
            options=['*', 'host', 'jid', 'type', 'event', 'unit', 'value', 'diff', 'arc'], value=['*'],
            description='Columns:'
        )
        self.columns_dropdown_hosts = widgets.Dropdown(
            options=['host', 'jid', 'type', 'event', 'unit', 'value', 'diff', 'arc'],
            description='Column:'
        )
        self.operators_dropdown_hosts = widgets.Dropdown(
            options=['=', '!=', '<', '>', '<=', '>=', 'LIKE'],
            description='Operator:'
        )
        self.value_input_hosts = widgets.Text(
            description='Value:'
        )
        self.start_time_hosts = widgets.NaiveDatetimePicker(
            value=datetime.now().replace(microsecond=0),
            description='Start Time:'
        )
        self.end_time_hosts = widgets.NaiveDatetimePicker(
            value=datetime.now().replace(microsecond=0),
            description='End Time:'
        )
        self.validate_button_hosts = widgets.Button(
            description="Validate Dates"
        )
        self.execute_button_hosts = widgets.Button(
            description="Execute Query"
        )
        self.add_condition_button_hosts = widgets.Button(
            description="Add Condition"
        )
        self.remove_condition_button_hosts = widgets.Button(
            description="Remove Condition"
        )
        self.condition_list_hosts = widgets.SelectMultiple(
            options=[], description='Conditions:'
        )
        self.distinct_checkbox = widgets.Checkbox(
            value=False,
            description='Select Distinct',
            disabled=False
        )
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
        self.limit_input = widgets.IntText(
            value=0,
            description='Limit Results:',
            disabled=False
        )
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

    def initialize_job_data_widgets(self):
        self.output_jobs = widgets.Output()
        self.query_output_jobs = widgets.Output()
        self.error_output_jobs = widgets.Output()
        self.banner_jobs = widgets.HTML("<h1>Query the Job Data Table</h1>")
        self.query_time_message_jobs = widgets.HTML(
            f"<h4>Select start and end times (Max: <b>{self.MAX_DAYS_JOBS}</b> days).</h4>")
        self.job_data_columns_dropdown = widgets.SelectMultiple(
            options=['*', 'jid', 'submit_time', 'start_time', 'end_time', 'runtime', 'timelimit', 'node_hrs',
                     'nhosts',
                     'ncores', 'ngpus', 'username', 'account', 'queue', 'state', 'jobname', 'exitcode',
                     'host_list'],
            value=['*'], description='Columns:'
        )
        self.data_filtering_cols_dropdown_jobs = widgets.Dropdown(
            options=['jid', 'submit_time', 'start_time', 'end_time', 'runtime', 'timelimit', 'node_hrs', 'nhosts',
                     'ncores',
                     'ngpus', 'username', 'account', 'queue', 'state', 'jobname', 'exitcode', 'host_list'],
            description='Column:'
        )
        self.operators_dropdown_jobs = widgets.Dropdown(
            options=['=', '!=', '<', '>', '<=', '>=', 'LIKE'],
            description='Operator:'
        )
        self.value_input_jobs = widgets.Text(
            description='Value:'
        )
        self.start_time_jobs = widgets.NaiveDatetimePicker(
            value=datetime.now().replace(microsecond=0), description='Start Time:'
        )
        self.end_time_jobs = widgets.NaiveDatetimePicker(
            value=datetime.now().replace(microsecond=0), description='End Time:'
        )
        self.validate_button_jobs = widgets.Button(
            description="Validate Dates"
        )
        self.execute_button_jobs = widgets.Button(
            description="Execute Query"
        )
        self.add_condition_button_jobs = widgets.Button(
            description="Add Condition"
        )
        self.remove_condition_button_jobs = widgets.Button(
            description="Remove Condition"
        )
        self.condition_list_jobs = widgets.SelectMultiple(
            options=[], description='Conditions:'
        )
        self.distinct_checkbox_jobs = widgets.Checkbox(
            value=False,
            description='Select Distinct',
            disabled=False
        )
        self.limit_input_jobs = widgets.IntText(
            value=0,
            description='Limit Results:',
            disabled=False
        )
        self.order_by_dropdown_jobs = widgets.Dropdown(
            options=['None'] + [col for col in self.job_data_columns_dropdown.options if col != "*"],
            value='None',
            description='Order By:'
        )
        self.order_by_direction_dropdown_jobs = widgets.Dropdown(
            options=['ASC', 'DESC'],
            value='ASC',
            description='Direction:'
        )
        self.in_values_dropdown_jobs = widgets.Dropdown(
            options=['None'] + [col for col in self.job_data_columns_dropdown.options if col != "*"],
            value='None',
            description='IN Column:'
        )

        self.in_values_textarea_jobs = widgets.Textarea(
            value='',
            placeholder='Enter values separated by commas',
            description='IN values:',
            disabled=False
        )

    def initialize_stats_widgets(self):
        self.stats = widgets.SelectMultiple()
        self.ratio_threshold = widgets.IntText()
        self.interval_type = widgets.Dropdown()
        self.time_units = widgets.Dropdown()
        self.time_value = widgets.IntText()

    def group_and_display_widgets(self):
        # Host Data stuff
        self.value_input_container_hosts = widgets.HBox([self.value_input_hosts])
        self.condition_buttons = widgets.HBox([self.add_condition_button_hosts, self.remove_condition_button_hosts])
        hosts_group = widgets.VBox([
            self.banner_hosts_message,
            self.query_time_message_hosts,
            self.start_time_hosts,
            self.end_time_hosts,
            self.validate_button_hosts,
            self.query_cols_message,
            self.host_data_columns_dropdown,
            self.distinct_checkbox,
            self.order_by_message,
            self.order_by_dropdown,
            self.order_by_direction_dropdown,
            self.limit_message,
            self.limit_input,
            self.in_values_message,
            self.in_values_dropdown,
            self.in_values_textarea,
            self.request_filters_message,
            self.columns_dropdown_hosts,
            self.operators_dropdown_hosts,
            self.value_input_container_hosts,
            self.condition_buttons,
            self.current_filters_message,
            self.condition_list_hosts,
            self.error_output_hosts,
            self.execute_button_hosts,
            self.query_output_hosts,
            self.output_hosts
        ])

        # Job Data stuff
        self.value_input_container_jobs = widgets.HBox([self.value_input_jobs])
        self.condition_buttons_jobs = widgets.HBox(
            [self.add_condition_button_jobs, self.remove_condition_button_jobs])
        jobs_group = widgets.VBox([
            self.banner_jobs,
            self.query_time_message_jobs,
            self.start_time_jobs,
            self.end_time_jobs,
            self.validate_button_jobs,
            self.query_cols_message,
            self.job_data_columns_dropdown,
            self.distinct_checkbox_jobs,
            self.order_by_message,
            self.order_by_dropdown_jobs,
            self.order_by_direction_dropdown_jobs,
            self.limit_message,
            self.limit_input_jobs,
            self.in_values_message,
            self.in_values_dropdown_jobs,
            self.in_values_textarea_jobs,
            self.request_filters_message,
            self.data_filtering_cols_dropdown_jobs,
            self.operators_dropdown_jobs,
            self.value_input_container_jobs,
            self.condition_buttons_jobs,
            self.current_filters_message,
            self.condition_list_jobs,
            self.error_output_jobs,
            self.execute_button_jobs,
            self.query_output_jobs,
            self.output_jobs
        ])

        # Combine in GridBox to place the two VBox widgets side by side
        grid = widgets.GridBox(
            children=[hosts_group, jobs_group],
            layout=widgets.Layout(
                width='100%',
                grid_template_columns='50% 50%',
            )
        )

        # Display
        display(grid)

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

            self.data_processor.remove_columns()
        except NameError:
            print("ERROR: Please make sure to run the previous notebook cell before executing this one.")

    def display_plots(self):
        try:
            display_plots = DisplayPlots(
                time_series_df=self.time_series_df,
                data_processor=self.data_processor,
                plotting_service=self.plotting_service,
                host_data_sql_query=self.host_data_sql_query,
                stats=self.stats,
                interval_type=self.interval_type,
                time_value=self.time_value,
                time_units=self.time_units,
                ratio_threshold=self.ratio_threshold
            )
            display_plots.display_plots()
        except NameError as e:
            print("ERROR: Please make sure to run the previous notebook cells before executing this one.")

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
        query, params = self.data_processor.construct_job_data_query(self.where_conditions_jobs,
                                                      self.job_data_columns_dropdown.value,
                                                      self.validate_button_jobs.description,
                                                      self.start_time_jobs.value,
                                                      self.end_time_jobs.value)
        with self.query_output_jobs:
            clear_output(wait=True)
            display(widgets.HTML("<h4>Current SQL query:</h4>"))
            print(f"{query}\nParameters: {params}")

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
        query, params = self.data_processor.construct_query_hosts(self.where_conditions_hosts,
                                                   self.host_data_columns_dropdown.value,
                                                   self.validate_button_hosts.description,
                                                   self.start_time_hosts.value,
                                                   self.end_time_hosts.value)
        with self.query_output_hosts:
            clear_output(wait=True)
            display(widgets.HTML("<h4>Current SQL query:</h4>"))
            print(f"{query}\nParameters: {params}")
            self.host_data_sql_query = query, params

    def pearson_correlation(self):
        try:
            def on_selection_change(change):
                if len(change.new) > 2:
                    correlations.value = change.new[:2]

            def on_button_click(button):
                graph_output.clear_output()
                with graph_output:
                    correlation_data = self.plotting_service.plot_correlation(correlations.value, self.time_series_df)
                    # Added feedback based on the result of plot_correlation
                    if correlation_data is None:
                        print("Unable to calculate correlation for the selected metrics. Please check the data or "
                              "select different metrics.")
                    else:
                        with plt.style.context('fivethirtyeight'):
                            display(correlation_data)

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

    def conditionally_display_legend(self):
        """
        This is a utility function to conditionally display the legend only if there are labeled data series.
        """
        if len(plt.gca().get_legend_handles_labels()[0]) > 0:
            plt.legend(loc='upper left', fontsize="10")
