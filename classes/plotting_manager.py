import warnings
import seaborn as sns
from classes.data_processor import DataProcessor
from matplotlib import pyplot as plt
import pandas as pd


class PlottingManager:
    def __init__(self, base_widget_manager):
        self.data_processor = DataProcessor(base_widget_manager)

    def plot_correlation(self, correlations, ts_df):
        """
        Plot the Pearson Correlation Coefficient.

        Parameters:
        correlation_data: A dictionary containing the Pearson Correlation Coefficient and p-value.

        Returns:
        None
        """
        correlation_data = self.data_processor.calculate_correlation(ts_df, correlations)

        if correlation_data is None:
            print("No data to plot.")
            return

        correlation = correlation_data['Correlation']
        p_val = correlation_data['P-value']
        metric_one = correlation_data['Metric One']
        metric_two = correlation_data['Metric Two']

        # Create scatter plot
        plt.figure(figsize=(10, 6))
        plt.scatter(metric_one, metric_two)
        plt.xlabel(metric_one)
        plt.ylabel(metric_two)
        plt.title(f'Scatter plot of {metric_one} and {metric_two}')

        # Add text box with the correlation and p-value
        text_str = f'Correlation: {correlation:.10f}\nP-value: {p_val:.10f}'
        plt.text(0.05, 0.95, text_str, transform=plt.gca().transAxes, fontsize=14,
                 verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

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
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            time_series_df = ts_df.dropna()
            sns.histplot(time_series_df['value'], cumulative=True, element="step", fill=False)
            plt.title('Cumulative Distribution Function (CDF)')
            plt.show()

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
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            time_series_df = ts_df.dropna()
            sns.histplot(time_series_df['value'], kde=True)
            plt.title('Probability Density Function (PDF)')
            plt.show()

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
