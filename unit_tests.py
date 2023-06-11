import os
import unittest

import notebook_functions as nbf
import unittest


class TestGetDataFilesDirectory(unittest.TestCase):
    def setUp(self) -> None:
        if os.name == "nt":
            os.mkdir("C:\\home\\user\\data\\")
            os.mkdir("C:\\home\\user\\data\\subdir\\")
        else:
            os.mkdir("/home/user/data/")
            os.mkdir("/home/user/data/subdir/")

    def tearDown(self) -> None:
        if os.name == "nt":
            os.rmdir("C:\\home\\user\\data\\")
            os.rmdir("C:\\home\\user\\data\\subdir\\")
        else:
            os.rmdir("/home/user/data/")
            os.rmdir("/home/user/data/subdir/")

    #  Tests that a valid directory path returns a string ending with a forward slash
    def test_valid_directory_path(self):
        if os.name == "nt":
            path = "C:\\home\\user\\data\\"
            assert nbf.get_data_files_directory(path) == "C:/home/user/data/"
        path = "/home/user/data/"
        assert nbf.get_data_files_directory(path) == "/home/user/data/"

    #  Tests that a valid subdirectory path returns a string ending with a forward slash
    def test_valid_subdirectory_path(self):
        if os.name == "nt":
            path = "C:\\home\\user\\data\\subdir\\"
            assert nbf.get_data_files_directory(path) == "C:/home/user/data/subdir/"
        path = "/home/user/data/subdir/"
        assert nbf.get_data_files_directory(path) == "/home/user/data/subdir/"

    #  Tests that an empty path returns an empty string
    def test_empty_path(self):
        path = ""
        assert nbf.get_data_files_directory(path) == ""

    #  Tests that a non-existent directory path returns an empty string
    def test_nonexistent_directory_path(self, mocker):
        path = "/home/user/nonexistent/"
        mocker.patch("os.path.exists", return_value=False)
        assert nbf.get_data_files_directory(path) == ""

    #  Tests that a file path returns an empty string
    def test_file_path(self, mocker):
        path = "/home/user/data/file.txt"
        mocker.patch("os.path.isdir", return_value=False)
        assert nbf.get_data_files_directory(path) == ""


if __name__ == "__main__":
    unittest.main()
