
import os
from datetime import datetime
import pandas as pd

from azutil.helper import get_results
# from filters import get_outfilename_for_filters


def write_df_to_csv(df, file_path:str, filters:dict|None={}):
  df['ResultsTimestamp'] = datetime.now().replace(microsecond=0)
  # Check if file exists
  if os.path.isfile(file_path):
      # If the file exists, append without writing the header
      df.to_csv(file_path, mode='a', index=False, header=False)
  else:
      # If the file does not exist, write the DataFrame to CSV with a header
      df.to_csv(file_path, index=False, header=True)  

  # df.to_csv(file_path, index=False, mode='a')


def add_filter_columns(df1, filters:dict):
  if not filters or not any(filters.values()):
    return df1
  
  df = df1.copy()
  
  # add filter columns to DataFrame
  for filter_name, filter_values in filters.items():
    # join the list of filter values into a single string with semicolon
    filter_values_str = '; '.join(filter_values)
    df[filter_name] = filter_values_str

  return df


def write_results_to_files(all_results, results_filename):# filters:dict = {}):
  
  # filt_fname = get_outfilename_for_filters(filters)

  for results in all_results:
    # title_for_file = results['title'].replace(" ", "_")
    data = results['data']
    data.insert(0,'ChartingDomain', results['title'])
    
    # data = add_filter_columns(data, filters)

    # results_filepath = f"{results_folder}{fname}_{title_for_file}.csv"
    # results_filepath = f"{results_folder}.csv"
    write_df_to_csv(data, results_filename)# results_filepath, filters)
    


# def write_df_to_csv(df, file_path:str, filters:dict|None={}):
#   df['ResultsTimestamp'] = datetime.now().replace(microsecond=0)
  
#   if not filters or not any(filters.values()):    
#     df.to_csv(file_path, index=False)
#     return
  
#   # add filter columns to DataFrame
#   for filter_name, filter_values in filters.items():
#     # join the list of filter values into a single string with semicolon
#     filter_values_str = '; '.join(filter_values)
#     df[filter_name] = filter_values_str

#   df.to_csv(file_path, index=False)

# def write_results_to_csv(results, file_path:str):
#   with open(file_path, 'w', newline='') as csvfile:
#     writer = csv.writer(csvfile)
#     writer.writerow(results[0].keys())
#     for row in results:
#       writer.writerow(row.values())

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
