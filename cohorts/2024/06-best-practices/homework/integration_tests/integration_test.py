import pytest
import os

import pandas as pd
from datetime import datetime

def dt(hour, minute, second=0) -> datetime:
    return datetime(2023, 1, 1, hour, minute, second)

@pytest.fixture
def mock_data() -> pd.DataFrame:
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)
    return df

@pytest.fixture
def s3_input(mock_data: pd.DataFrame):
    options = {
        'client_kwargs': {
            'endpoint_url': 'http://localhost:4566'
        }
    }

    input_file = "s3://nyc-duration/mock_2024_03.parquet"

    mock_data.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

def test_batch_integration(s3_input: None) -> None:
    assert True
    os.environ['S3_ENDPOINT_URL'] = "http://localhost:4566"
    os.environ['INPUT_FILE_PATTERN'] = "s3://nyc-duration/mock_{year:04d}_{month:02d}.parquet"
    os.environ['OUTPUT_FILE_PATTERN'] = "s3://nyc-duration/mock_output_{year:04d}_{month:02d}.parquet"

    os.system('python batch.py 2024 03')

    df = pd.read_parquet(
        "s3://nyc-duration/mock_output_2024_03.parquet", 
        storage_options={
            'client_kwargs': {
                'endpoint_url': "http://localhost:4566"
            }
        }
    )

    print(f"Total duration = {df['predicted_duration'].sum()}")

    assert len(df) == 2

    
