import argparse
from notebook_functions import get_time_series_from_database


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('start_time', type=str, help='The start time in the format of %Y-%m-%d %H:%M:%S')
    parser.add_argument('end_time', type=str, help='The end time in the format of %Y-%m-%d %H:%M:%S')

    args = parser.parse_args()

    df = get_time_series_from_database(args.start_time, args.end_time)

    print(df)


if __name__ == "__main__":
    main()
