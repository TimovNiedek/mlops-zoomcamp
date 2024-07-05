import pytest
from batch import prepare_data

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
def expected_output() -> pd.DataFrame:
    data = [
        ("-1", "-1", dt(1, 1), dt(1, 10), 9.0),
        ("1", "1", dt(1, 2), dt(1, 10), 8.0),
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    df = pd.DataFrame(data, columns=columns)
    return df


def test_prepare_data(mock_data: pd.DataFrame, expected_output: pd.DataFrame) -> None:
    df = prepare_data(mock_data, ['PULocationID', 'DOLocationID'])
    print(df.head())
    assert (df == expected_output).all().all()
    
