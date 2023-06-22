import os
import notebook_functions as nbf
import unittest
from unittest.mock import patch, Mock
import pandas as pd
from IPython.display import FileLink
from pandas.testing import assert_frame_equal


class NotebookFunctionsUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pd.DataFrame({
            'Host': ['NODE83', 'NODE86', 'NODE85', 'NODE100'],
            'Data': [10, 20, 30, 40]
        })

        self.job_data = pd.DataFrame({
            'Job Id': ['JOB1', 'JOB2', 'JOB3', 'JOB4'],
            'Data': [10, 20, 30, 40]
        })

        self.time_series = pd.DataFrame({
            'Job Id': ['JOB1', 'JOB2', 'JOB3']
        })

        self.account_log = pd.DataFrame({
            'Job Id': ['JOB1', 'JOB2', 'JOB3', 'JOB4', 'JOB5'],
            'Data': [10, 20, 30, 40, 50]
        })

    def tearDown(self) -> None:
        pass

    def test_edge_case_same_start_end_time_missing_metric(self):
        # create a mock CSV file with sample data
        csv_data = {'Timestamp': ['2022-01-01 00:00:00', '2022-01-01 01:00:00', '2022-01-01 02:00:00'],
                    'Metric1': [1, 2, 3],
                    'Metric2': [4, 5, 6]}
        mock_csv = pd.DataFrame(csv_data)

        # save the mock CSV file to a temporary directory
        temp_dir = os.path.join(os.getcwd(), 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        mock_csv.to_csv(os.path.join(temp_dir, 'job_ts_metrics_jan2022_anon.csv'), index=False)

        # call the function with the same start and end time
        result = nbf.handle_missing_metrics('2022-01-01 00:00:00', '2022-01-01 00:00:00', temp_dir)

        # assert that the result is an empty pandas DataFrame
        self.assertEqual(result, None)

    def test_edge_case_file_not_found_missing_metrics(self):
        # call the function with a non-existent directory path
        with self.assertRaises(FileNotFoundError):
            nbf.handle_missing_metrics('2022-01-01 00:00:00', '2022-01-01 02:00:00', 'nonexistent/path')

    #  Tests that the function correctly extracts the month and year from a valid date string
    def test_happy_path_valid_date(self):
        date_string = '2022-01-01 12:00:00'
        expected_month = 'Jan'
        expected_year = '2022'

        month, year = nbf.extract_month_year(date_string)

        self.assertEqual(month, expected_month)
        self.assertEqual(year, expected_year)

    #  Tests that the function raises a ValueError when given an empty date string
    def test_edge_case_empty_date_string(self):
        date_string = ''

        with self.assertRaises(ValueError):
            nbf.extract_month_year(date_string)

    #  Tests that the function raises a ValueError when given a date string with missing parts
    def test_edge_case_missing_parts_date_string(self):
        date_string = '2022-01-01'

        with self.assertRaises(ValueError):
            nbf.extract_month_year(date_string)

    #  Tests that the function raises a ValueError when given a date string with non-numeric characters
    def test_edge_case_non_numeric_characters_date_string(self):
        date_string = '2022-01-01 ab:cd:ef'

        with self.assertRaises(ValueError):
            nbf.extract_month_year(date_string)

    #  Tests that the function correctly handles leap years
    def test_general_behaviour_leap_year(self):
        date_string = '2024-02-29 12:00:00'
        expected_month = 'Feb'
        expected_year = '2024'

        month, year = nbf.extract_month_year(date_string)

        self.assertEqual(month, expected_month)
        self.assertEqual(year, expected_year)

    def test_happy_path(self):
        # Arrange
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        title = "Download CSV file"
        filename = "data.csv"

        # Act
        result = nbf.create_download_link(df, title, filename)

        # Assert
        self.assertIsInstance(result, FileLink)

    def test_happy_path_s(self):
        input_str = "This is a test, 123."
        expected_output = "Thisisatest,123"
        self.assertEqual(nbf.remove_special_chars(input_str), expected_output)

    def test_empty_input(self):
        input_str = ""
        expected_output = ""
        self.assertEqual(nbf.remove_special_chars(input_str), expected_output)

    def test_special_characters_only(self):
        input_str = "!@#$%^&*()"
        expected_output = ""
        self.assertEqual(nbf.remove_special_chars(input_str), expected_output)

    def test_single_host(self):
        hosts = 'NODE83'
        expected_result = pd.DataFrame({
            'Host': ['NODE83'],
            'Data': [10]
        })
        result = nbf.get_timeseries_by_hosts(hosts, self.data)
        assert_frame_equal(result, expected_result)

    def test_multiple_hosts(self):
        hosts = 'NODE83,NODE86'
        expected_result = pd.DataFrame({
            'Host': ['NODE83', 'NODE86'],
            'Data': [10, 20]
        })
        result = nbf.get_timeseries_by_hosts(hosts, self.data)
        assert_frame_equal(result, expected_result)

    def test_single_job_id(self):
        job_ids = 'JOB1'
        expected_result = pd.DataFrame({
            'Job Id': ['JOB1'],
            'Data': [10]
        })
        result = nbf.get_timeseries_by_job_ids(job_ids, self.job_data)
        assert_frame_equal(result, expected_result)

    def test_multiple_job_ids(self):
        job_ids = 'JOB1,JOB2'
        expected_result = pd.DataFrame({
            'Job Id': ['JOB1', 'JOB2'],
            'Data': [10, 20]
        })
        result = nbf.get_timeseries_by_job_ids(job_ids, self.job_data)
        assert_frame_equal(result, expected_result)


if __name__ == '__main__':
    unittest.main(verbosity=2, buffer=True)
