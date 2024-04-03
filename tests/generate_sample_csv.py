import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_ROOT = Path(__file__).resolve().parent

if __name__ == '__main__':
    df_latest = pd.read_csv(f'{PROJECT_ROOT}/data/epimetric_latest.csv')
    df_full = pd.read_csv(f'{PROJECT_ROOT}/data/epimetric_full.csv')
    df_latest_sample = df_latest.sample(n=10)
    df_full_sample = df_full.merge(df_latest_sample, how="left",
                                   on=["signal_key_id", "geo_key_id", "time_type", "time_value"], indicator=True).query(
        '_merge=="both"')
    df_latest_sample.to_csv(f'{TEST_ROOT}/data/epimetric_latest_sample.csv', index=False)
    df_full_sample.to_csv(f'{TEST_ROOT}/data/epimetric_full_sample.csv', index=False)