import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
KEYS = ["signal_key_id", "geo_key_id", "time_type", "time_value"]

def get_latest_from_df_full(df_full: pd.DataFrame) -> pd.DataFrame:
    df_full_latest_partial = df_full.groupby(by=["signal_key_id", "geo_key_id", "time_type", "time_value"],
                                             group_keys=False, as_index=False).agg({'issue': "max"})
    df_full_latest = df_full.merge(df_full_latest_partial,
                                   on=["signal_key_id", "geo_key_id", "time_type", "time_value", "issue"], how="inner")
    return df_full_latest

def get_out_of_date_entries(df_full: pd.DataFrame, df_latest: pd.DataFrame) -> pd.DataFrame:
    # Identify the entried in df_latest that aren't the latest in df_full
    # Return a dataframe of the links that are out of date
    # ps. might want to think about how you could use this at the end of the program for testing
    df_full_latest = get_latest_from_df_full(df_full)
    df_latest_ood = df_latest.merge(df_full_latest, how="outer", indicator=True).query('_merge=="left_only"')
    df_latest_ood = df_latest_ood.drop('_merge', axis=1)
    return df_latest_ood

def get_latest_entries(df_full: pd.DataFrame, df_ood: pd.DataFrame) -> pd.DataFrame:
    # Identify the latest entries in df_full for the entries that are in df_ood (df out of date)
    # Return a dataframe of the updates entries for df_ood
    df_full_latest = get_latest_from_df_full(df_full)
    df_latest_patch = df_full_latest.merge(df_ood, on=["signal_key_id", "geo_key_id", "time_type", "time_value"], how="inner", suffixes=('', "_remove"))
    cols_to_drop = df_latest_patch.columns[df_latest_patch.columns.str.contains('_remove')]
    df_latest_patch.drop(cols_to_drop, axis=1, inplace=True)
    return df_latest_patch


def update_epimetric_latest(df_latest: pd.DataFrame, df_updates: pd.DataFrame) -> pd.DataFrame:
    # Update the entries in df_latest with the values in df_updates where the keys match
    # Return a dataframe of all entries of df_latest including the updates
    # ps. Assure that the size of the dataframe did not change

    before_shape = df_latest.shape
    df_latest.update(df_updates, overwrite=True)
    assert before_shape == df_latest.shape
    return df_latest



if __name__ == '__main__':
    df_latest = pd.read_csv(f'{PROJECT_ROOT}/data/epimetric_latest.csv')
    df_full = pd.read_csv(f'{PROJECT_ROOT}/data/epimetric_full.csv')

    # *** CALL YOUR FUNCTIONS HERE ***
    # output your resulting dataframe to a CSV
    df_latest_ood = get_out_of_date_entries(df_full, df_latest)
    df_latest_updates = get_latest_entries(df_full, df_latest_ood)
    patched_df_latest = update_epimetric_latest(df_latest, df_latest_updates)
    patched_df_latest.to_csv(f'{PROJECT_ROOT}/data/epimetric_latest_fixed.csv', index=False)