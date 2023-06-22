import pandas as pd

def convert_to_datetime(df, column_names:list[str]|str):
  df [column_names] = pd.to_datetime(df[column_names])
