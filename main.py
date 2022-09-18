import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

START_DATE = '2021-01-01'
INPUT_FILE = Path('data/Meter.db')
OUTPUT_FILE = Path('data/cycling.json')


def load_data(data_file: Path, start_date: str) -> pd.DataFrame:
    query = f"""
    SELECT
        runID as run_id,
        startTime as start_time,
        distance / 1000 AS distance,
        runTime as run_time
    FROM run
    WHERE start_time>'{start_date}'
    """

    engine = create_engine(f'sqlite:///{INPUT_FILE}')
    return pd.read_sql(
        query,
        engine,
        parse_dates=['start_time']
    ).set_index('run_id').sort_values(by='start_time')


def main():
    data = load_data(INPUT_FILE, START_DATE)

    result = {'monthly_sums': {}}
    for year, year_data in data.groupby(data.start_time.dt.year):
        monthly_sums = year_data.groupby(year_data.start_time.dt.month).sum()

        result['monthly_sums'][str(year)] = {
            f'{year}-{str(month).zfill(2)}-01': distance
            for month, distance in monthly_sums.to_dict()['distance'].items()
        }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(result, f)


if __name__ == '__main__':
    main()
