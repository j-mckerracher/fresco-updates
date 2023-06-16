import os

import notebook_functions as nbf
import unittest
from unittest.mock import patch, Mock
import pandas as pd
from IPython.display import display, FileLink
from ipywidgets import widgets


class NotebookFunctionsUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

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


if __name__ == '__main__':
    unittest.main(verbosity=2, buffer=True)
