import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def convert_host_data(csv_file_path):
    # Load CSV data
    df = pd.read_csv(f"time_series/{csv_file_path}")

    # Rename columns to match the target schema
    df = df.rename(columns={
        'Timestamp': 'time',
        'Host': 'host',
        'Job Id': 'jid',
        'Event': 'event',
        'Value': 'value',
        'Units': 'unit'
    })

    # Set the correct data types
    df['time'] = pd.to_datetime(df['time'])
    df['host'] = df['host'].astype('string')
    df['jid'] = df['jid'].astype('string')
    df['event'] = df['event'].astype('string')
    df['value'] = df['value'].astype('float32')
    df['unit'] = df['unit'].astype('string')

    # Convert to Parquet format
    parquet_file_path = f'host_data_parquet/{csv_file_path[:-4]}.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file_path)

    print(f'host_data converted to Parquet and saved at {parquet_file_path}')


for file in os.listdir("time_series"):
    convert_host_data(file)


def convert_job_data(csv_file_path):
    # Load CSV data
    df = pd.read_csv(f"job_accounting/{csv_file_path}")

    # Rename columns to match the target schema
    df = df.rename(columns={
        'Submit Time': 'submit_time',
        'Start Time': 'start_time',
        'End Time': 'end_time',
        'Cpu Time': 'runtime',
        'Requested Wall Time': 'timelimit',
        'Node Time': 'node_hrs',
        'Nodes': 'nhosts',
        'Cores': 'ncores',
        'Gpus': 'ngpus',
        'User': 'username',
        'Account': 'account',
        'Queue': 'queue',
        'Exit Status': 'exitcode',
        'Hosts': 'host_list',
        'Job Name': 'jobname'
    })

    # Set the correct data types
    df['submit_time'] = pd.to_datetime(df['submit_time'])
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    df['runtime'] = df['runtime'].astype('float32')
    df['timelimit'] = df['timelimit'].astype('float32')
    df['node_hrs'] = df['node_hrs'].astype('float32')
    df['nhosts'] = df['nhosts'].astype('int32')
    df['ncores'] = df['ncores'].astype('int32')
    df['ngpus'] = df['ngpus'].astype('int32')
    df['username'] = df['username'].astype('string')
    df['account'] = df['account'].astype('string')
    df['queue'] = df['queue'].astype('string')
    df['exitcode'] = df['exitcode'].astype('string')
    df['host_list'] = df['host_list'].apply(lambda x: x.split(','))
    df['jobname'] = df['jobname'].astype('string')

    # Convert to Parquet format
    parquet_file_path = f'job_data_parquet/{csv_file_path[:-4]}.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file_path)

    print(f'job_data converted to Parquet and saved at {parquet_file_path}')


for file in os.listdir("job_accounting"):
    convert_job_data(file)
