import argparse
import notebook_functions as nbf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('start_time', type=str, help='The start time in the format of %Y-%m-%d %H:%M:%S')
    parser.add_argument('end_time', type=str, help='The end time in the format of %Y-%m-%d %H:%M:%S')

    args = parser.parse_args()

    df = nbf.get_time_series_from_database(args.start_time, args.end_time)

    print("time series:")
    print(df)

    df2 = nbf.get_account_log_from_database(args.start_time, args.end_time)
    print("account log:")
    print(df2)


if __name__ == "__main__":
    main()
