
import os
import pandas as pd

from azutil.helper import get_results
from .df_ops import read_parquet 

def get_data(start_date, end_date, download_filepath:str, cache=False):
  #
  # get from ATOM Azure DB and save to disk
  #
  if cache:
    if os.path.exists(f"{download_filepath}.parquet"):
      print("INFO: Using cached data")
      df = read_parquet(f"{download_filepath}.parquet")
      return df
    else:
      print("INFO: No cached data found, loading from DB")

  results = get_results(start_date, end_date)
  if not results:
    print("ERROR : Zero results")
    return None
  
  df = pd.DataFrame.from_records(results)
  df.to_parquet(f"{download_filepath}.parquet")
  return df
