#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import os

from typing import List


S3_ENDPOINT_URL: str | None = os.getenv('S3_ENDPOINT_URL')


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-tvn/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def read_data(filename: str) -> pd.DataFrame:
    if S3_ENDPOINT_URL is None:
        return pd.read_parquet(filename)
    else:
        return pd.read_parquet(
            filename, 
            storage_options={
                'client_kwargs': {
                    'endpoint_url': S3_ENDPOINT_URL
                }
            }
        )


def save_data(filename: str, df: pd.DataFrame) -> None:
    if S3_ENDPOINT_URL is None:
        df_result.to_parquet(output_file, engine='pyarrow', index=False)
    else:
        return df.to_parquet(
            filename,
            engine='pyarrow',
            compression=None,
            index=False,
            storage_options={
                'client_kwargs': {
                    'endpoint_url': S3_ENDPOINT_URL
                }
            }
        )



def prepare_data(df: pd.DataFrame, categorical_cols: List[str]) -> pd.DataFrame:
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical_cols] = df[categorical_cols].fillna(-1).astype('int').astype('str')
    return df


def main(year: int, month: int) -> None:
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)


    categorical_cols = ['PULocationID', 'DOLocationID']

    df = read_data(input_file)
    df = prepare_data(df, categorical_cols)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical_cols].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())


    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    save_data(output_file, df_result)



if __name__ == '__main__':
    main(int(sys.argv[1]), int(sys.argv[2]))
