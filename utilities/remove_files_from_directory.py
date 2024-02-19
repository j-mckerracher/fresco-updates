import os
import time

output_dir = "temp_output"
file_life_in_seconds = 30  # 3600


def remove_all_files_from_directory(directory):
    if os.path.exists(directory):  # Check if the directory exists
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            if os.path.isfile(file_path):  # Check if it is a file
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error occurred while trying to remove file {file_path}. Error: {str(e)}")
    else:
        print(f"The directory {directory} does not exist.")


if __name__ == "__main__":
    print(f"Removing all files from directory {output_dir}")
    time.sleep(file_life_in_seconds)
    remove_all_files_from_directory(output_dir)
