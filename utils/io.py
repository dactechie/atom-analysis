
import os
import pandas as pd

from azutil.helper import get_results


def read_parquet(file_path:str) -> pd.DataFrame|None:
  if os.path.exists(file_path):
    df = pd.read_parquet(file_path)
    return df
  return None


def get_data(start_date, end_date, download_filepath:str, cache=False) -> pd.DataFrame|None:
  #
  # get from ATOM Azure DB and save to disk
  #
  if cache:
    if os.path.exists(f"{download_filepath}"):
      print("INFO: Using cached data")
      df = read_parquet(f"{download_filepath}")
      return df
    else:
      print("INFO: No cached data found, loading from DB")

  results = get_results(start_date, end_date)
  if not results:
    print("ERROR : Zero results")
    return None
  
  df = pd.DataFrame.from_records(results)
  df.to_parquet(f"{download_filepath}")
  return df
