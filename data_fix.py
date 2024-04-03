import pandas as pd


def get_out_of_date_entries(df_full: pd.DataFrame, df_latest: pd.DataFrame) -> pd.DataFrame:
    # Identify the entried in df_latest that aren't the latest in df_full
    # Return a dataframe of the links that are out of date
    pass
    # ps. might want to think about how you could use this at the end of the program for testing

def get_latest_entries(df_full: pd.DataFrame, df_ood: pd.DataFrame) -> pd.DataFrame:
    # Identify the latest entries in df_full for the entries that are in df_ood (df out of date)
    # Return a dataframe of the updates entries for df_ood
    pass

def update_epimetric_latest(df_latest: pd.DataFrame, df_updates: pd.DataFrame) -> pd.DataFrame:
    # Update the entries in df_latest with the values in df_updates where the keys match
    # Return a dataframe of all entries of df_latest including the updates
    pass
    # ps. Assure that the size of the dataframe did not change

if __name__ == '__main__':
    df_latest = pd.read_csv('./data/epimetric_latest.csv')
    df_full = pd.read_csv('./data/epimetric_all.csv')

    # *** CALL YOUR FUNCTIONS HERE ***

    # output your resulting dataframe to a CSV
    somedataframe.to_csv('./data/epimetric_latest_fixed.csv', index=False)