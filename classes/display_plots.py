from abc import ABC, abstractmethod
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import ipywidgets as widgets
from IPython.display import display


class DataProcessor(ABC):
    @abstractmethod
    def parse_host_data_query(self, query):
        pass

    @abstractmethod
    def get_mean(self, df, rolling=False, window=None):
        pass

    @abstractmethod
    def get_median(self, df, rolling=False, window=None):
        pass

    @abstractmethod
    def get_standard_deviation(self, df, rolling=False, window=None):
        pass


class PlottingService(ABC):
    @abstractmethod
    def plot_pdf(self, df):
        pass

    @abstractmethod
    def plot_cdf(self, df):
        pass

    @abstractmethod
    def plot_data_points_outside_threshold(self, threshold, df):
        pass

    @abstractmethod
    def plot_box_and_whisker(self, df_mean, df_std, df_median):
        pass

    @abstractmethod
    def conditionally_display_legend(self):
        pass


class DisplayPlots:
    def __init__(self, time_series_df, data_processor, plotting_service, host_data_sql_query, stats, interval_type,
                 time_value, time_units, ratio_threshold):
        self.time_series_df = time_series_df
        self.data_processor = data_processor
        self.plotting_service = plotting_service
        self.host_data_sql_query = host_data_sql_query
        self.stats = stats
        self.interval_type = interval_type
        self.time_value = time_value
        self.time_units = time_units
        self.ratio_threshold = ratio_threshold

    def display_plots(self):
        try:
            ts_df = self.time_series_df.copy()
            try:
                ts_df['time'] = pd.to_datetime(ts_df['time'])
                ts_df = ts_df.set_index('time')
                ts_df = ts_df.sort_index()
            except Exception as e:
                print("")

            metric_func_map = {
                "Mean": self.data_processor.get_mean,
                "Median": self.data_processor.get_median,
                "Standard Deviation": self.data_processor.get_standard_deviation,
                "PDF": self.plotting_service.plot_pdf,
                "CDF": self.plotting_service.plot_cdf,
                "Ratio of Data Outside Threshold": self.plotting_service.plot_data_points_outside_threshold
            }

            unit_map = {
                "CPU %": "cpuuser",
                "GPU %": "gpu_usage",
                "GB:memused": "memused",
                "GB:memused_minus_diskcache": "memused_minus_diskcache",
                "GB/s": "block",
                "MB/s": "nfs"
            }

            units = self.data_processor.parse_host_data_query(self.host_data_sql_query)

            total_operations = len(units) * len(self.stats.value)
            pbar = tqdm(total=total_operations, desc="Generating chart/s")

            tab = widgets.Tab()
            outputs = self._initialize_outputs(units)

            self._set_tab_children(tab, outputs, units)
            message_display = widgets.HTML(value="Initializing . . .")
            display(message_display)

            with plt.style.context('fivethirtyeight'):
                unit_stat_dfs = self._calculate_unit_stats(ts_df, units, unit_map, metric_func_map, outputs, pbar)
                self._plot_box_and_whisker(units, unit_stat_dfs, outputs, ts_df, unit_map)

            message_display.value = "Plotting complete"
            display(tab)
            pbar.close()
        except NameError as e:
            print("ERROR: Please make sure to run the previous notebook cells before executing this one.")

    def _initialize_outputs(self, units):
        outputs = {}
        basic_stats = ['Mean', 'Median', 'Standard Deviation']

        for unit in units:
            outputs[unit] = {}
            if any(stat in self.stats.value for stat in basic_stats):
                for stat in self.stats.value + ('Box and Whisker',):
                    outputs[unit][stat] = widgets.Output()
            else:
                for stat in self.stats.value:
                    outputs[unit][stat] = widgets.Output()

        return outputs

    def _set_tab_children(self, tab, outputs, units):
        basic_stats = ['Mean', 'Median', 'Standard Deviation']

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

    def _calculate_unit_stats(self, ts_df, units, unit_map, metric_func_map, outputs, pbar):
        unit_stat_dfs = {}
        time_map = {'Days': 'D', 'Hours': 'H', 'Minutes': 'T', 'Seconds': 'S'}

        for unit in units:
            unit_stat_dfs[unit] = {}
            for metric in self.stats.value:
                metric_df = ts_df.query(f"`event` == '{unit_map[unit]}'")
                rolling = False

                if self.interval_type.value == "Time":
                    rolling = True
                    try:
                        window = f"{self.time_value.value}{time_map[self.time_units.value]}"
                    except KeyError:
                        print("Error! Please ensure a selection was made in the 'Interval Unit' dropdown.")
                elif self.interval_type.value == "Count":
                    rolling = True
                    window = self.time_value.value

                if metric in ["PDF", "CDF", "Ratio of Data Outside Threshold"]:
                    with outputs[unit][metric]:
                        unit_stat_dfs[unit][metric] = self._handle_special_cases(metric, metric_func_map, metric_df)
                    continue

                pbar.update(1)

                if rolling:
                    unit_stat_dfs[unit][metric] = metric_func_map[metric](metric_df, rolling=True, window=window)
                    self._plot_rolling_stats(unit, metric, unit_stat_dfs, outputs)
                else:
                    self._plot_entire_metric(unit, metric, metric_df, outputs)

        return unit_stat_dfs

    def _handle_special_cases(self, metric, metric_func_map, metric_df):
        if metric == "PDF":
            return metric_func_map[metric](metric_df)
        elif metric == "CDF":
            return metric_func_map[metric](metric_df)
        elif metric == "Ratio of Data Outside Threshold":
            return metric_func_map[metric](self.ratio_threshold.value, metric_df)

    def _plot_rolling_stats(self, unit, metric, unit_stat_dfs, outputs):
        with outputs[unit][metric]:
            unit_stat_dfs[unit][metric].plot()
            x_axis_label = ""
            if self.interval_type.value == "Count":
                x_axis_label += f"Count - Rolling Window: {self.time_value.value} Rows"
            elif self.interval_type.value == "Time":
                x_axis_label += f"Timestamp - Rolling Window: {self.time_value.value}{self.time_units.value}"
            y_axis_label = unit
            plt.gcf().autofmt_xdate()
            plt.style.use('fivethirtyeight')
            plt.title(f"{unit} {metric}")
            self.plotting_service.conditionally_display_legend()
            plt.xlabel(x_axis_label)
            plt.ylabel(y_axis_label)
            plt.show()

    def _plot_entire_metric(self, unit, metric, metric_df, outputs):
        with outputs[unit][metric]:
            metric_df['value'].plot()
            plt.gcf().autofmt_xdate()
            plt.style.use('fivethirtyeight')
            plt.title(f"{unit} {metric} Over Time")
            plt.xlabel("Timestamp")
            plt.ylabel(unit)

            stat_value = self._calculate_stat_value(metric, metric_df)
            if stat_value is not None:
                annotation_text = f"{metric}: {stat_value:.2f}"
                plt.annotate(annotation_text, xy=(0.05, 0.95), xycoords='axes fraction', fontsize=10,
                             verticalalignment='top', bbox=dict(boxstyle="square", facecolor="white"))

            plt.show()

    def _calculate_stat_value(self, metric, metric_df):
        if metric == "Mean":
            return metric_df['value'].mean()
        elif metric == "Median":
            return metric_df['value'].median()
        elif metric == "Standard Deviation":
            return metric_df['value'].std()
        else:
            return None

    def _plot_box_and_whisker(self, units, unit_stat_dfs, outputs, ts_df, unit_map):
        for unit in units:
            df_mean = unit_stat_dfs[unit].get('Mean')
            df_std = unit_stat_dfs[unit].get('Standard Deviation')
            df_median = unit_stat_dfs[unit].get('Median')

            if not any(df is not None for df in [df_mean, df_std, df_median]):
                metric_df = ts_df.query(f"`event` == '{unit_map[unit]}'")
                if 'Mean' in self.stats.value:
                    df_mean = pd.DataFrame(metric_df['value'])
                    df_mean.columns = ['value']
                if 'Standard Deviation' in self.stats.value:
                    df_std = pd.DataFrame(metric_df['value'])
                    df_std.columns = ['value']
                if 'Median' in self.stats.value:
                    df_median = pd.DataFrame(metric_df['value'])
                    df_median.columns = ['value']

            if any(df is not None for df in [df_mean, df_std, df_median]):
                with outputs[unit]['Box and Whisker']:
                    self.plotting_service.plot_box_and_whisker(df_mean, df_std, df_median)
