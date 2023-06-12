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

    def test_happy_path(self):
        # Arrange
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        title = "Download CSV file"
        filename = "data.csv"

        # Act
        result = nbf.create_download_link(df, title, filename)

        # Assert
        self.assertIsInstance(result, FileLink)


if '__name__' == '__main__':
    unittest.main(verbosity=2, buffer=True)
