import pytest
import pandas as pd
from pathlib import Path
from data_fix import get_latest_from_df_full, get_out_of_date_entries, get_latest_entries, update_epimetric_latest

TEST_ROOT = Path(__file__).resolve().parent
class TestDataFix:

    df_latest = pd.read_csv(f'{TEST_ROOT}/data/epimetric_latest_sample.csv', index_col=False)
    df_full = pd.read_csv(f'{TEST_ROOT}/data/epimetric_full_sample.csv', index_col=False)

    def test_get_out_of_date_entries(self) -> pd.DataFrame:
        # Identify the entried in df_latest that aren't the latest in df_full
        # Return a dataframe of the links that are out of date
        # ps. might want to think about how you could use this at the end of the program for testing
        df_latest_ood = get_out_of_date_entries(self.df_full, self.df_latest)
        assert df_latest_ood.isnull

    def test_get_latest_entries(df_full: pd.DataFrame, df_ood: pd.DataFrame) -> pd.DataFrame:
        # Identify the latest entries in df_full for the entries that are in df_ood (df out of date)
        # Return a dataframe of the updates entries for df_ood
        df_full_latest = get_latest_from_df_full(df_full)
        df_latest_patch = df_full_latest.merge(df_ood, on=["signal_key_id", "geo_key_id", "time_type", "time_value"],
                                               how="inner", suffixes=('', "_remove"))
        cols_to_drop = df_latest_patch.columns[df_latest_patch.columns.str.contains('_remove')]
        df_latest_patch.drop(cols_to_drop, axis=1, inplace=True)
        return df_latest_patch

    def test_update_epimetric_latest(df_latest: pd.DataFrame, df_updates: pd.DataFrame) -> pd.DataFrame:
        pass

