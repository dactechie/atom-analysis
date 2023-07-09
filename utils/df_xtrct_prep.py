
#from typing import TYPE_CHECKING
#if TYPE_CHECKING:
# import os

from utils.io import get_data, read_parquet
from data_prep import prep_dataframe, limit_min_num_assessments, limit_clients_active_inperiod


# if there is no pre-processed data, then extract and process it
# Processing it includes:
#   - dropping fields that are not needed
#   - limiting to clients who have at least 1 assessment in the period of interest
#   - limiting to clients who have completed at least 3 surveys
#   - converting data types
#   - normalizing data (if there are nested data structures like SurveyData)
#   - caching the processed version
def extract_prep_data(extract_start_date, extract_end_date
                      , active_clients_start_date
                      , active_clients_end_date
                      , fname) :#-> pd.DataFrame|None:
  
  MIN_NUM_ATOMS_PER_CLIENT = 3
  processed_filepath = f"./data/processed/processed_all_cols_{fname}.parquet"
  processed_df = read_parquet(processed_filepath)
  
  if not(isinstance(processed_df, type(None)) or processed_df.empty):
    return processed_df
  
  print("INFO: No processed data found, loading from raw data.")    
  raw_df = get_data(extract_start_date, extract_end_date, f"./data/in/{fname}.parquet", cache=True)
  
  if isinstance(raw_df, type(None)) or raw_df.empty:
    print("ERROR: No data found")
    exit(1)
  
  # Clean and Transform the dataset
  processed_df = prep_dataframe(raw_df) # only one filter: PDCSubstanceOrGambling has to have a value

  processed_df = limit_clients_active_inperiod(processed_df, active_clients_start_date, active_clients_end_date)
  # Limit to only clients who have completed at least 3 survey during the period of interest.
  processed_df = limit_min_num_assessments(processed_df, MIN_NUM_ATOMS_PER_CLIENT)
  
  # cache the processed data
  processed_df.to_parquet(f"{processed_filepath}")
  
  return processed_df

