import pandas as pd


# -------------- CELL 2 --------------
def get_data_files_directory(path) -> str:
    """
    This function should produce a folder path to the data files.
    :param path:
    :return:
    """
    pass


# -------------- CELL 4 --------------
def handle_missing_metrics(starting_time, ending_time, path):
    """
    This function should remove the rows within the given timeframe that are missing metrics.
    :param starting_time:
    :param ending_time:
    :param path:
    :return:
    """
    pass


def add_interval_column(starting_time, ending_time, path):
    """
    This function should add an interval column to the data that falls within the given timeframe. The interval column
    should reflect the length of each timestamp.
    :param starting_time:
    :param ending_time:
    :param path:
    :return:
    """
    pass


# -------------- CELL 6 --------------

def get_timeseries_by_timestamp(begin_time: str, end_time: str, return_columns: list) -> pd.DataFrame:
    pass


def get_timeseries_by_values_and_unit(units: str, low_value, high_value) -> pd.DataFrame:
    pass


def get_timeseries_by_hosts(hosts: str) -> pd.DataFrame:
    pass


def get_timeseries_by_job_ids(job_ids: str) -> pd.DataFrame:
    pass


def get_account_logs_by_job_ids(job_ids: str) -> pd.DataFrame:
    pass


# -------------- CELL 7 --------------
def get_average():
    pass


def get_mean():
    pass


def get_median():
    pass


def get_standard_deviation():
    pass


def get_probability_density():
    pass


def get_cumulative_density():
    pass


def get_data_points_outside_threshold():
    pass


def get_ratio_of_data_points_outside_threshold():
    pass


def calculate_correlation():
    pass


# -------------- CELL 8 --------------
# TBD

# -------------- CELL 10 --------------
def get_files():
    pass
