import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def get_latest_entries(df_full: pd.DataFrame) -> pd.DataFrame:
    # Identify the latest entries in df_full
    df_full_latest_partial = df_full.groupby(by=["signal_key_id", "geo_key_id", "time_type", "time_value"],
                                             group_keys=False, as_index=False).agg({'issue': "max"})
    df_full_latest = df_full.merge(df_full_latest_partial,
                                   on=["signal_key_id", "geo_key_id", "time_type", "time_value", "issue"], how="inner")
    return df_full_latest


def get_out_of_date_entries(df_full: pd.DataFrame, df_latest: pd.DataFrame) -> pd.DataFrame:
    # Identify the entried in df_latest that aren't the latest in df_full
    # Return a dataframe of the links that are out of date
    # ps. might want to think about how you could use this at the end of the program for testing
    df_full_latest = get_latest_entries(df_full)
    df_latest_diff = df_latest.merge(df_full_latest, how="outer", indicator=True).query('_merge=="left_only"')
    return df_latest_diff


def update_epimetric_latest(df_latest: pd.DataFrame, df_updates: pd.DataFrame) -> pd.DataFrame:
    # Update the entries in df_latest with the values in df_updates where the keys match
    # Return a dataframe of all entries of df_latest including the updates

    # ps. Assure that the size of the dataframe did not change

if __name__ == '__main__':
    df_latest = pd.read_csv(f'{PROJECT_ROOT}/data/epimetric_latest.csv')
    df_full = pd.read_csv(f'{PROJECT_ROOT}/data/epimetric_full.csv')

    df_latest_sample = df_latest.sample(n=10)
    df_full_sample = df_full.merge(df_latest_sample, how="left", on=["signal_key_id", "geo_key_id", "time_type", "time_value"], indicator=True).query('_merge=="both"')
    # *** CALL YOUR FUNCTIONS HERE ***
    # output your resulting dataframe to a CSV
    missing_latest_df = get_out_of_date_entries(df_full, df_latest)
    # somedataframe.to_csv('./data/epimetric_latest_fixed.csv', index=False)