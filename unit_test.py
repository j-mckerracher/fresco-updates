import os
from unittest.mock import patch, Mock, MagicMock
import psycopg2
import notebook_functions as nbf
import unittest
import pandas as pd


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

        self.sample_data = pd.DataFrame({
            'jid': [1, 1, 2, 2],
            'host': ['A', 'A', 'B', 'B'],
            'event': ['E1', 'E2', 'E1', 'E2'],
            'unit': ['U1', 'U2', 'U1', 'U2'],
            'value': [10, 20, 30, 40]
        })

        # Sample DataFrames for testing
        self.df_with_columns = pd.DataFrame({
            "id": [1, 2, 3],
            "type": ["A", "B", "C"],
            "value": [10, 20, 30],
            "diff": [0.1, 0.2, 0.3],
            "arc": [5, 6, 7]
        })

        self.df_without_columns = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })

    def tearDown(self) -> None:
        pass

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

    @patch('notebook_functions.os.getenv')
    @patch('notebook_functions.psycopg2.connect')
    def test_successful_connection(self, mock_connect, mock_getenv):
        # Mocking the environment variables
        mock_getenv.side_effect = ['host', 'password', 'dbname', 'user']

        # Mocking a successful database connection
        mock_connect.return_value = Mock(spec=psycopg2.extensions.connection)

        connection = nbf.get_database_connection()
        self.assertIsNotNone(connection, "Expected a connection object, but got None.")

    @patch('notebook_functions.os.getenv')
    def test_missing_environment_variables(self, mock_getenv):
        # Mocking missing environment variables
        mock_getenv.side_effect = [None, 'password', 'dbname', 'user']

        connection = nbf.get_database_connection()
        self.assertIsNone(connection,
                          "Expected None due to missing environment variables, but got a connection object.")

    @patch('notebook_functions.os.getenv')
    @patch('notebook_functions.psycopg2.connect')
    def test_database_connection_error(self, mock_connect, mock_getenv):
        # Mocking the environment variables
        mock_getenv.side_effect = ['host', 'password', 'dbname', 'user']

        # Mocking a database connection error
        mock_connect.side_effect = psycopg2.OperationalError("Failed to connect.")

        connection = nbf.get_database_connection()
        self.assertIsNone(connection, "Expected None due to a connection error, but got a connection object.")

    @patch('notebook_functions.get_database_connection')
    @patch('notebook_functions.pd.read_sql')
    def test_successful_query_execution(self, mock_read_sql, mock_get_database_connection):
        # Mocking a successful database connection with MagicMock
        conn_mock = MagicMock()
        mock_get_database_connection.return_value = conn_mock

        # Mocking a successful query execution
        mock_read_sql.return_value = pd.DataFrame()

        result = nbf.execute_sql_query("SELECT * FROM table_name", pd.DataFrame())
        self.assertIsInstance(result, pd.DataFrame, "Expected a DataFrame, but did not get one.")

    @patch('notebook_functions.get_database_connection')
    def test_database_connection_failure(self, mock_get_database_connection):
        # Mocking a failed database connection
        mock_get_database_connection.return_value = None

        result = nbf.execute_sql_query("SELECT * FROM table_name", pd.DataFrame())
        self.assertIsNone(result, "Expected None due to connection failure, but got a result.")

    @patch('notebook_functions.get_database_connection')
    @patch('notebook_functions.pd.read_sql')
    def test_invalid_sql_query(self, mock_read_sql, mock_get_database_connection):
        # Mocking a successful database connection
        conn_mock = Mock()
        mock_get_database_connection.return_value = conn_mock

        # Mocking an error during query execution
        mock_read_sql.side_effect = Exception("Invalid SQL query.")

        result = nbf.execute_sql_query("INVALID QUERY", pd.DataFrame())
        self.assertIsNone(result, "Expected None due to an invalid query, but got a result.")

    @patch('notebook_functions.get_database_connection')
    @patch('notebook_functions.pd.read_sql')
    def test_parameterized_query(self, mock_read_sql, mock_get_database_connection):
        # Mocking a successful database connection with MagicMock
        conn_mock = MagicMock()
        mock_get_database_connection.return_value = conn_mock

        # Mocking a successful query execution
        df_result = pd.DataFrame()
        mock_read_sql.return_value = df_result

        params = {'param1': 'value1'}
        result = nbf.execute_sql_query("SELECT * FROM table_name WHERE column1 = :param1", pd.DataFrame(),
                                       params=params)

        mock_read_sql.assert_called_once_with("SELECT * FROM table_name WHERE column1 = :param1",
                                              conn_mock.__enter__.return_value, params=params)

        self.assertTrue(result.equals(df_result), "Expected the mocked DataFrame, but got a different result.")

    def test_valid_jid_format(self):
        valid_jids = ["JOB123", "JOB1", "JOB45678"]
        for jid in valid_jids:
            result = nbf.validate_jid(jid)
            self.assertIsNone(result, f"Expected a valid jid format for {jid}, but got an error: {result}")

    def test_invalid_jid_format(self):
        invalid_jids = ["JOB", "JOBABC", "123JOB", "JOB1234A"]
        for jid in invalid_jids:
            result = nbf.validate_jid(jid)
            expected_error = "Error: For 'jid', value must be a comma-separated list of strings starting with 'JOB' " \
                             "followed by one or more digits."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {jid}, but got a different message: {result}")

    def test_multiple_jids(self):
        jid_string = "JOB123, JOB456, JOB789"
        result = nbf.validate_jid(jid_string)
        self.assertIsNone(result, f"Expected a valid jid format for {jid_string}, but got an error: {result}")

        jid_string_invalid = "JOB123, ABC, JOB789"
        result = nbf.validate_jid(jid_string_invalid)
        expected_error = "Error: For 'jid', value must be a comma-separated list of strings starting with 'JOB' " \
                         "followed by one or more digits."
        self.assertEqual(result, expected_error,
                         f"Expected an error for {jid_string_invalid}, but got a different message: {result}")

    def test_valid_numeric_value(self):
        valid_values = ["123", "0.456", "78.90", "-123.45"]
        for column in ["ncores", "ngpus", "nhosts", "timelimit"]:
            for value in valid_values:
                result = nbf.validate_numeric_columns(column, value)
                self.assertIsNone(result,
                                  f"Expected a valid numeric format for column {column} and value {value}, but got an error: {result}")

    def test_invalid_numeric_value(self):
        invalid_values = ["ABC", "123ABC", "78.90.123"]
        for column in ["ncores", "ngpus", "nhosts", "timelimit"]:
            for value in invalid_values:
                result = nbf.validate_numeric_columns(column, value)
                expected_error = f"Error: For '{column}', value must be a number (including decimals)."
                self.assertEqual(result, expected_error,
                                 f"Expected an error for column {column} and value {value}, but got a different message: {result}")

    def test_different_columns(self):
        valid_value = "123.45"
        columns = ["ncores", "ngpus", "nhosts", "timelimit"]
        for column in columns:
            result = nbf.validate_numeric_columns(column, valid_value)
            self.assertIsNone(result,
                              f"Expected a valid numeric format for column {column}, but got an error: {result}")

        invalid_value = "ABC123"
        for column in columns:
            result = nbf.validate_numeric_columns(column, invalid_value)
            expected_error = f"Error: For '{column}', value must be a number (including decimals)."
            self.assertEqual(result, expected_error,
                             f"Expected an error for column {column}, but got a different message: {result}")

    def test_valid_account_format(self):
        valid_accounts = ["GROUP123", "GROUP1", "GROUP45678"]
        for account in valid_accounts:
            result = nbf.validate_account(account)
            self.assertIsNone(result, f"Expected a valid account format for {account}, but got an error: {result}")

    def test_invalid_account_format(self):
        invalid_accounts = ["GROUP", "GROUPABC", "123GROUP", "GROUP1234A"]
        for account in invalid_accounts:
            result = nbf.validate_account(account)
            expected_error = "Error: For 'account', value must be a comma-separated list of strings starting with " \
                             "'GROUP' followed by one or more digits."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {account}, but got a different message: {result}")

    def test_multiple_accounts(self):
        account_string = "GROUP123, GROUP456, GROUP789"
        result = nbf.validate_account(account_string)
        self.assertIsNone(result, f"Expected a valid account format for {account_string}, but got an error: {result}")

        account_string_invalid = "GROUP123, ABC, GROUP789"
        result = nbf.validate_account(account_string_invalid)
        expected_error = "Error: For 'account', value must be a comma-separated list of strings starting with 'GROUP' " \
                         "followed by one or more digits."
        self.assertEqual(result, expected_error,
                         f"Expected an error for {account_string_invalid}, but got a different message: {result}")

    def test_valid_username_format(self):
        valid_usernames = ["USER123", "USER1", "USER45678"]
        for username in valid_usernames:
            result = nbf.validate_username(username)
            self.assertIsNone(result, f"Expected a valid username format for {username}, but got an error: {result}")

    def test_invalid_username_format(self):
        invalid_usernames = ["USER", "USERABC", "123USER", "USER1234A"]
        for username in invalid_usernames:
            result = nbf.validate_username(username)
            expected_error = "Error: For 'username', value must be a comma-separated list of strings starting with " \
                             "'USER' followed by one or more digits."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {username}, but got a different message: {result}")

    def test_multiple_usernames(self):
        username_string = "USER123, USER456, USER789"
        result = nbf.validate_username(username_string)
        self.assertIsNone(result, f"Expected a valid username format for {username_string}, but got an error: {result}")

        username_string_invalid = "USER123, ABC, USER789"
        result = nbf.validate_username(username_string_invalid)
        expected_error = "Error: For 'username', value must be a comma-separated list of strings starting with 'USER' " \
                         "followed by one or more digits."
        self.assertEqual(result, expected_error,
                         f"Expected an error for {username_string_invalid}, but got a different message: {result}")

    def test_valid_host_list_format(self):
        valid_hosts = ["NODE123", "NODE1", "NODE45678"]
        for host in valid_hosts:
            result = nbf.validate_host_list(host)
            self.assertIsNone(result, f"Expected a valid host format for {host}, but got an error: {result}")

    def test_invalid_host_list_format(self):
        invalid_hosts = ["NODE", "NODEABC", "123NODE", "NODE1234A"]
        for host in invalid_hosts:
            result = nbf.validate_host_list(host)
            expected_error = "Error: For 'host_list', value must be a comma-separated list of strings starting with " \
                             "'NODE' followed by one or more digits."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {host}, but got a different message: {result}")

    def test_multiple_hosts(self):
        host_string = "NODE123, NODE456, NODE789"
        result = nbf.validate_host_list(host_string)
        self.assertIsNone(result, f"Expected a valid host format for {host_string}, but got an error: {result}")

        host_string_invalid = "NODE123, ABC, NODE789"
        result = nbf.validate_host_list(host_string_invalid)
        expected_error = "Error: For 'host_list', value must be a comma-separated list of strings starting with " \
                         "'NODE' followed by one or more digits."
        self.assertEqual(result, expected_error,
                         f"Expected an error for {host_string_invalid}, but got a different message: {result}")

    def test_valid_jobname_format(self):
        valid_jobnames = ["JOBNAME123", "JOBNAME1", "JOBNAME45678"]
        for jobname in valid_jobnames:
            result = nbf.validate_jobname(jobname)
            self.assertIsNone(result, f"Expected a valid jobname format for {jobname}, but got an error: {result}")

    def test_invalid_jobname_format(self):
        invalid_jobnames = ["JOBNAME", "JOBNAMEABC", "123JOBNAME", "JOBNAME1234A"]
        for jobname in invalid_jobnames:
            result = nbf.validate_jobname(jobname)
            expected_error = "Error: For job name, value must be a comma-separated list of strings starting with " \
                             "'JOBNAME' followed by one or more digits."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {jobname}, but got a different message: {result}")

    def test_multiple_jobnames(self):
        jobname_string = "JOBNAME123, JOBNAME456, JOBNAME789"
        result = nbf.validate_jobname(jobname_string)
        self.assertIsNone(result, f"Expected a valid jobname format for {jobname_string}, but got an error: {result}")

        jobname_string_invalid = "JOBNAME123, ABC, JOBNAME789"
        result = nbf.validate_jobname(jobname_string_invalid)
        expected_error = "Error: For job name, value must be a comma-separated list of strings starting with " \
                         "'JOBNAME' followed by one or more digits."
        self.assertEqual(result, expected_error,
                         f"Expected an error for {jobname_string_invalid}, but got a different message: {result}")

    def test_valid_values(self):
        valid_values = {
            'jid': 'JOB123,JOB456',
            'ncores': '5',
            'ngpus': '3.5',
            'nhosts': '10',
            'timelimit': '120.5',
            'account': 'GROUP123',
            'username': 'USER456',
            'host_list': 'NODE789',
            'jobname': 'JOBNAME012'
        }
        for column, value in valid_values.items():
            result = nbf.validate_condition_jobs(column, value)
            self.assertIsNone(result,
                              f"Expected a valid format for column '{column}' and value '{value}', but got an error: {result}")

    def test_invalid_values(self):
        invalid_values = {
            'jid': 'JOB, JOB456',
            'ncores': 'five',
            'ngpus': 'three.point.five',
            'nhosts': 'ten',
            'timelimit': 'two.hundred',
            'account': 'GROUP',
            'username': 'USER',
            'host_list': 'NODE',
            'jobname': 'JOBNAME'
        }
        for column, value in invalid_values.items():
            result = nbf.validate_condition_jobs(column, value)
            self.assertIsNotNone(result,
                                 f"Expected an error for column '{column}' and value '{value}', but got no error.")

    def test_unknown_column(self):
        unknown_column = 'unknown_column'
        value = 'some_value'
        result = nbf.validate_condition_jobs(unknown_column, value)
        self.assertIsNone(result,
                          f"Expected no error for unknown column '{unknown_column}', but got an error: {result}")

    def test_valid_values(self):
        # Assuming sample valid values. You can replace these with your actual valid values.
        valid_values = {
            'event': 'block',
            'host': 'NODE123',
            'jid': 'JOB123,JOB456',
            'unit': 'CPU %',
            'value': '5.5'
        }
        for column, value in valid_values.items():
            result = nbf.validate_condition_hosts(column, value)
            self.assertIsNone(result,
                              f"Expected a valid format for column '{column}' and value '{value}', but got an error: {result}")

    def test_invalid_values(self):
        # Assuming sample invalid values. You can replace these with your actual invalid values.
        invalid_values = {
            'event': '123EVENT',
            'host': '123HOST',
            'jid': 'JOB, JOB456',
            'unit': '123UNIT',
            'value': 'five.point.five'
        }
        for column, value in invalid_values.items():
            result = nbf.validate_condition_hosts(column, value)
            self.assertIsNotNone(result,
                                 f"Expected an error for column '{column}' and value '{value}', but got no error.")

    def test_unknown_column(self):
        unknown_column = 'unknown_column'
        value = 'some_value'
        result = nbf.validate_condition_hosts(unknown_column, value)
        self.assertIsNone(result,
                          f"Expected no error for unknown column '{unknown_column}', but got an error: {result}")

    def test_valid_event_values(self):
        valid_events = ['cpuuser', 'block', 'memused', 'memused_minus_diskcache', 'gpu_usage', 'nfs']
        for event in valid_events:
            result = nbf.validate_event(event)
            self.assertIsNone(result, f"Expected a valid event format for {event}, but got an error: {result}")

    def test_invalid_event_values(self):
        invalid_events = ['cpu', 'memory', 'gpu', 'network']
        for event in invalid_events:
            result = nbf.validate_event(event)
            expected_error = "Error: For 'event', value must be one of: cpuuser, block, memused, " \
                             "memused_minus_diskcache, gpu_usage, nfs."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {event}, but got a different message: {result}")

    def test_valid_host_format(self):
        valid_hosts = ["NODE123", "NODE1", "NODE45678"]
        for host in valid_hosts:
            result = nbf.validate_host(host)
            self.assertIsNone(result, f"Expected a valid host format for {host}, but got an error: {result}")

    def test_invalid_host_format(self):
        invalid_hosts = ["NODE", "NODEABC", "123NODE", "NODE1234A"]
        for host in invalid_hosts:
            result = nbf.validate_host(host)
            expected_error = "Error: For 'host', value must be a comma-separated list of strings starting with 'NODE' " \
                             "followed by one or more digits."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {host}, but got a different message: {result}")

    def test_multiple_hosts(self):
        host_string = "NODE123, NODE456, NODE789"
        result = nbf.validate_host(host_string)
        self.assertIsNone(result, f"Expected a valid host format for {host_string}, but got an error: {result}")

        host_string_invalid = "NODE123, ABC, NODE789"
        result = nbf.validate_host(host_string_invalid)
        expected_error = "Error: For 'host', value must be a comma-separated list of strings starting with 'NODE' " \
                         "followed by one or more digits."
        self.assertEqual(result, expected_error,
                         f"Expected an error for {host_string_invalid}, but got a different message: {result}")

    def test_valid_unit_values(self):
        valid_units = ['CPU %', 'GPU %', 'GB:memused', 'GB:memused_minus_diskcache', 'GB/s', 'MB/s']
        for unit in valid_units:
            result = nbf.validate_unit(unit)
            self.assertIsNone(result, f"Expected a valid unit format for {unit}, but got an error: {result}")

    def test_invalid_unit_values(self):
        invalid_units = ['CPU', 'GPU', 'GB', 'MB', 'GB:mem', 'MB/speed']
        for unit in invalid_units:
            result = nbf.validate_unit(unit)
            expected_error = "Error: For 'unit', value must be one of: 'CPU %', 'GPU %', 'GB:memused', " \
                             "'GB:memused_minus_diskcache', 'GB/s', 'MB/s'."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {unit}, but got a different message: {result}")

    def test_valid_numeric_values(self):
        valid_values = ["123", "1.23", "-123", "-1.23", "0.0", "0"]
        for value in valid_values:
            result = nbf.validate_value(value)
            self.assertIsNone(result, f"Expected a valid numeric format for {value}, but got an error: {result}")

    def test_invalid_numeric_values(self):
        invalid_values = ["abc", "123a", "1..23", "--123", "1/2", "2*3"]
        for value in invalid_values:
            result = nbf.validate_value(value)
            expected_error = "Error: For 'value', the value must be a number."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {value}, but got a different message: {result}")

    def test_valid_jid_format(self):
        valid_jids = ["JOB123", "JOB1", "JOB45678"]
        for jid in valid_jids:
            result = nbf.validate_jid(jid)
            self.assertIsNone(result, f"Expected a valid jid format for {jid}, but got an error: {result}")

    def test_invalid_jid_format(self):
        invalid_jids = ["JOB", "JOBABC", "123JOB", "JOB1234A"]
        for jid in invalid_jids:
            result = nbf.validate_jid(jid)
            expected_error = "Error: For 'jid', value must be a comma-separated list of strings starting with 'JOB' " \
                             "followed by one or more digits."
            self.assertEqual(result, expected_error,
                             f"Expected an error for {jid}, but got a different message: {result}")

    def test_multiple_jids(self):
        jid_string = "JOB123,JOB456, JOB789"
        result = nbf.validate_jid(jid_string)
        self.assertIsNone(result, f"Expected a valid jid format for {jid_string}, but got an error: {result}")

        jid_string_invalid = "JOB123, ABC, JOB789"
        result = nbf.validate_jid(jid_string_invalid)
        expected_error = "Error: For 'jid', value must be a comma-separated list of strings starting with 'JOB' " \
                         "followed by one or more digits."
        self.assertEqual(result, expected_error,
                         f"Expected an error for {jid_string_invalid}, but got a different message: {result}")

    def test_basic_query_structure(self):
        query, params = nbf.construct_query_hosts([], ['column1', 'column2'], "", None, None)
        expected_query = "SELECT column1, column2 FROM host_data"
        self.assertEqual(query, expected_query)
        self.assertEqual(params, [])

    def test_conditions_in_query(self):
        where_conditions = [('column1', '=', 'value1'), ('column2', '<>', 'value2')]
        query, params = nbf.construct_query_hosts(where_conditions, ['column1', 'column2'], "", None, None)
        expected_query = "SELECT column1, column2 FROM host_data WHERE column1 = %s AND column2 <> %s"
        self.assertEqual(query, expected_query)
        self.assertEqual(params, ['value1', 'value2'])

    def test_time_filters_in_query(self):
        where_conditions = [('column1', '=', 'value1')]
        query, params = nbf.construct_query_hosts(where_conditions, ['column1', 'column2'], "Times Valid",
                                                  "2021-01-01 00:00:00", "2021-01-02 00:00:00")
        expected_query = "SELECT column1, column2 FROM host_data WHERE column1 = %s AND time BETWEEN %s AND %s"
        self.assertEqual(query, expected_query)
        self.assertEqual(params, ['value1', "2021-01-01 00:00:00", "2021-01-02 00:00:00"])

    def test_basic_query_structure_j(self):
        query, params = nbf.construct_job_data_query([], ['jid', 'runtime'], "", None, None)
        expected_query = "SELECT jid, runtime FROM job_data"
        self.assertEqual(query, expected_query)
        self.assertEqual(params, [])

    def test_conditions_in_query(self):
        where_conditions = [('jid', '=', 'JOB123'), ('runtime', '<', '120')]
        query, params = nbf.construct_job_data_query(where_conditions, ['jid', 'runtime'], "", None, None)
        expected_query = "SELECT jid, runtime FROM job_data WHERE jid = %s AND runtime < %s"
        self.assertEqual(query, expected_query)
        self.assertEqual(params, ['JOB123', '120'])

    def test_time_filters_in_query(self):
        where_conditions = [('jid', '=', 'JOB123')]
        query, params = nbf.construct_job_data_query(where_conditions, ['jid', 'runtime'], "Times Valid",
                                                     "2021-01-01 00:00:00", "2021-01-02 00:00:00")
        expected_query = "SELECT jid, runtime FROM job_data WHERE jid = %s AND start_time BETWEEN %s AND %s"
        self.assertEqual(query, expected_query)
        self.assertEqual(params, ['JOB123', "2021-01-01 00:00:00", "2021-01-02 00:00:00"])

    def test_basic_mean_calculation(self):
        data = {'value': [1, 2, 3, 4, 5],
                'jid': [1, 1, 1, 1, 1],
                'host': ['A', 'A', 'A', 'A', 'A'],
                'event': ['X', 'X', 'X', 'X', 'X'],
                'unit': ['%', '%', '%', '%', '%']}
        df = pd.DataFrame(data)
        result = nbf.get_mean(df)
        self.assertEqual(result['value'], 3.0)

    def test_rolling_mean_calculation(self):
        data = {'value': [1, 2, 3, 4, 5],
                'jid': [1, 1, 1, 1, 1],
                'host': ['A', 'A', 'A', 'A', 'A'],
                'event': ['X', 'X', 'X', 'X', 'X'],
                'unit': ['%', '%', '%', '%', '%']}
        df = pd.DataFrame(data)
        result = nbf.get_mean(df, rolling=True, window=2)  # Using window=2 instead of '2T'
        expected_result = pd.Series([None, 1.5, 2.5, 3.5, 4.5], name='value')
        pd.testing.assert_series_equal(result['value'], expected_result)

    # def test_no_data(self):
    #     df = pd.DataFrame()
    #     result = nbf.get_mean(df)
    #     self.assertTrue(result.empty)

    # def test_basic_median(self):
    #     data = {'value': [1, 2, 3, 4, 5]}
    #     df = pd.DataFrame(data)
    #     result = nbf.get_median(df)
    #     self.assertEqual(result['value'], 3)

    # def test_rolling_median(self):
    #     data = {'value': [1, 2, 3, 4, 5]}
    #     df = pd.DataFrame(data)
    #     result = nbf.get_median(df, rolling=True, window=2)
    #     expected_result = pd.Series([None, 1.5, 2.5, 3.5, 4.5], name='value')
    #     pd.testing.assert_series_equal(result['value'], expected_result)

    # def test_missing_columns(self):
    #     data = {'value': [1, 2, 3, 4, 5]}
    #     df = pd.DataFrame(data)
    #     result = nbf.get_median(df)
    #     self.assertEqual(result['value'], 3)

    def test_overall_standard_deviation(self):
        result = nbf.get_standard_deviation(self.sample_data)
        expected_result = self.sample_data['value'].std()
        self.assertAlmostEqual(result['value'], expected_result,
                               msg="The overall standard deviation calculation is incorrect.")

    def test_rolling_standard_deviation(self):
        result = nbf.get_standard_deviation(self.sample_data, rolling=True, window=2)  # Expect a DataFrame
        expected_result = self.sample_data['value'].rolling(window=2).std().to_frame()  # Convert Series to DataFrame
        pd.testing.assert_frame_equal(result, expected_result)  # Use assert_frame_equal

    def test_valid_query_with_matches(self):
        query = ("SELECT * FROM table WHERE unit = %s", ["metres"])
        units = {"metres": "m", "kilogram": "kg"}
        self.assertEqual(nbf.parse_host_data_query(query, units), ["metres"])

    def test_valid_query_without_matches(self):
        query = ("SELECT * FROM table WHERE unit = %s", ["inches"])
        units = {"metres": "m", "kilogram": "kg"}
        self.assertEqual(nbf.parse_host_data_query(query, units), ["metres", "kilogram"])

    def test_invalid_query_format(self):
        query = "SELECT * FROM table WHERE unit = %s"
        units = {"metres": "m", "kilogram": "kg"}
        self.assertEqual(nbf.parse_host_data_query(query, units), ["metres", "kilogram"])

    def test_query_with_few_elements(self):
        query = ("SELECT * FROM table WHERE unit = %s",)
        units = {"metres": "m", "kilogram": "kg"}
        self.assertEqual(nbf.parse_host_data_query(query, units), ["metres", "kilogram"])

    def test_empty_mapped_units(self):
        query = ("SELECT * FROM table WHERE unit = %s", ["metres"])
        units = {}
        self.assertEqual(nbf.parse_host_data_query(query, units), [])

    def test_query_with_exception_params(self):
        query = ("SELECT * FROM table WHERE unit = %s", {"metres": "m"})
        units = {"metres": "m", "kilogram": "kg"}
        self.assertEqual(nbf.parse_host_data_query(query, units), ["metres", "kilogram"])

    @patch.object(pd.DataFrame, 'to_csv', return_value="id,name\n1,John")
    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    @patch('zipfile.ZipFile')
    def test_valid_df_default_filename(self, mock_zip, mock_getcwd, mock_to_csv):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        expected_path = os.path.join("/tmp", "data.zip")
        self.assertEqual(nbf.create_csv_download_file(df), f"File saved to {expected_path}")

    @patch.object(pd.DataFrame, 'to_csv', return_value="id,name\n1,John")
    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    @patch('zipfile.ZipFile')
    def test_valid_df_custom_filename(self, mock_zip, mock_getcwd, mock_to_csv):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        expected_path = os.path.join("/tmp", "custom.zip")
        self.assertEqual(nbf.create_csv_download_file(df, "custom.csv"), f"File saved to {expected_path}")

    @patch.object(pd.DataFrame, 'to_csv', return_value="id,name\n1,John")
    @patch('os.getcwd', return_value="/tmp")
    @patch('zipfile.ZipFile', side_effect=Exception("Invalid file"))
    def test_df_invalid_filename(self, mock_zip, mock_getcwd, mock_to_csv):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        self.assertEqual(nbf.create_csv_download_file(df, "/invalid/path.csv"), "An error occurred: Invalid file")

    @patch.object(pd.DataFrame, 'to_csv', side_effect=Exception("Conversion error"))
    @patch('os.getcwd', return_value="/tmp")
    def test_df_to_csv_error(self, mock_getcwd, mock_to_csv):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        self.assertEqual(nbf.create_csv_download_file(df), "An error occurred: Conversion error")

    @patch('os.getcwd', return_value="/tmp")
    def test_non_dataframe_input(self, mock_getcwd):
        df = {"id": [1], "name": ["John"]}
        self.assertEqual(nbf.create_csv_download_file(df), "An error occurred: 'dict' object has no attribute 'to_csv'")

    @patch.object(pd.DataFrame, 'to_csv', return_value="id,name\n1,John\n2,ErrorChar")
    @patch('os.getcwd', return_value="/tmp")
    @patch('zipfile.ZipFile', side_effect=Exception("Encoding error"))
    def test_df_invalid_data(self, mock_zip, mock_getcwd, mock_to_csv):
        df = pd.DataFrame({"id": [1, 2], "name": ["John", "ErrorChar"]})
        result = nbf.create_csv_download_file(df)
        self.assertEqual(result, "An error occurred: Encoding error")

    @patch.object(pd.DataFrame, 'to_excel')
    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    @patch('zipfile.ZipFile')
    def test_valid_df_default_filename(self, mock_zip, mock_getcwd, mock_to_excel):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        expected_path = os.path.join("/tmp", "data.zip")
        self.assertEqual(nbf.create_excel_download_file(df), f"File saved to {expected_path}")

    @patch.object(pd.DataFrame, 'to_excel')
    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    @patch('zipfile.ZipFile')
    def test_valid_df_custom_filename(self, mock_zip, mock_getcwd, mock_to_excel):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        expected_path = os.path.join("/tmp", "custom.zip")
        self.assertEqual(nbf.create_excel_download_file(df, "custom.xlsx"), f"File saved to {expected_path}")

    @patch.object(pd.DataFrame, 'to_excel')
    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    @patch('zipfile.ZipFile')
    def test_df_with_timezone_aware_datetime(self, mock_zip, mock_getcwd, mock_to_excel):
        df = pd.DataFrame({
            "id": [1],
            "timestamp": [pd.Timestamp('2021-01-01 12:00:00', tz='US/Eastern')]
        })
        expected_path = os.path.join("/tmp", "data.zip")
        self.assertEqual(nbf.create_excel_download_file(df), f"File saved to {expected_path}")

    @patch.object(pd.DataFrame, 'to_excel', side_effect=Exception("Conversion error"))
    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    def test_df_to_excel_error(self, mock_getcwd, mock_to_excel):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        self.assertEqual(nbf.create_excel_download_file(df), "An error occurred: Conversion error")

    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    def test_non_dataframe_input(self, mock_getcwd):
        df = {"id": [1], "name": ["John"]}
        self.assertEqual(nbf.create_excel_download_file(df),
                         "An error occurred: 'dict' object has no attribute 'columns'")

    @patch.object(pd.DataFrame, 'to_excel')
    @patch('os.getcwd', return_value=os.path.join("/tmp"))
    @patch('zipfile.ZipFile', side_effect=Exception("Invalid file"))
    def test_df_invalid_filename(self, mock_zip, mock_getcwd, mock_to_excel):
        df = pd.DataFrame({"id": [1], "name": ["John"]})
        self.assertEqual(nbf.create_excel_download_file(df, "/invalid/path.xlsx"),
                         "An error occurred: Invalid file")

    def test_remove_existing_columns(self):
        result_df = nbf.remove_columns(self.df_with_columns)
        # Check if 'type', 'diff', and 'arc' columns are removed
        self.assertNotIn('type', result_df.columns)
        self.assertNotIn('diff', result_df.columns)
        self.assertNotIn('arc', result_df.columns)

    def test_remove_nonexistent_columns(self):
        result_df = nbf.remove_columns(self.df_without_columns)
        # Check if the resulting DataFrame remains unchanged
        pd.testing.assert_frame_equal(result_df, self.df_without_columns)

    def test_non_dataframe_input(self):
        with self.assertRaises(ValueError):
            nbf.remove_columns({"id": [1, 2, 3], "value": [10, 20, 30]})


if __name__ == '__main__':
    unittest.main(verbosity=2, buffer=True)
