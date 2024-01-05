
#from typing import TYPE_CHECKING
#if TYPE_CHECKING:
# import os
from typing import Literal
import mylogger
from utils.io import get_data, read_parquet, write_parquet, read_csv_to_dataframe
from data_prep import prep_dataframe, limit_min_num_assessments, limit_clients_active_inperiod, prep_dataframe_episodes

logger = mylogger.get(__name__)

# if there is no pre-processed data, then extract and process it
# Processing it includes:
#   - dropping fields that are not needed
#   - limiting to clients who have at least 1 assessment in the period of interest
#   - limiting to clients who have completed at least 3 surveys
#   - converting data types
#   - normalizing data (if there are nested data structures like SurveyData)
#   - caching the processed version
def extract_prep_atom_data(extract_start_date, extract_end_date
                      , active_clients_start_date
                      , active_clients_end_date
                      , fname, min_atoms_per_client = -1, purpose:Literal['NADA', 'Matching']='Matching') :#-> pd.DataFrame|None:
  
  processed_filepath = f"./data/processed/atom_{purpose}_{fname}.parquet"
  processed_df = read_parquet(processed_filepath)
  
  if not(isinstance(processed_df, type(None)) or processed_df.empty):
    logger.debug("found & returning pre-processed parquet file.")
    return processed_df
  
  logger.info("No processed data found, loading from raw data.")
  raw_df = get_data('ATOM',extract_start_date, extract_end_date, f"./data/in/atom_{purpose}_{fname}.parquet", cache=True)
  
  if isinstance(raw_df, type(None)) or raw_df.empty:
    logger.error("No data found. Exiting.")
    exit(1)
  
  # Clean and Transform the dataset
  processed_df = prep_dataframe(raw_df, prep_type=purpose) # only one filter: PDCSubstanceOrGambling has to have a value
  if active_clients_start_date and active_clients_end_date:
    processed_df = limit_clients_active_inperiod(processed_df, active_clients_start_date, active_clients_end_date)
    
  # Limit to only clients who have completed at least 3 survey during the period of interest.
  if purpose != 'NADA' and min_atoms_per_client > 0:
    processed_df = limit_min_num_assessments(processed_df, min_atoms_per_client)
  
  # cache the processed data
  # processed_df.to_parquet(f"{processed_filepath}")
  # try:
  #   write_parquet(processed_df, processed_filepath) # don't force overwrite
  #   logger.info(f"Done saving processed data to {processed_filepath}")
  # except ArrowTypeError as re:
  #   logger.error(f"ArrowTypeError: {re}. unable to save parquet file.")     
  # except Exception as ae:
  #   logger.error(f"ArrowTypeError: {ae}. unable to save parquet file.")    
  # finally:
  return processed_df

def read_and_prep_csv(fname):
  from utils.ccare_to_aztable import adjust_ccare_csv_for_aztable
  raw_df= read_csv_to_dataframe(f"./data/in/{fname}.csv")
  return adjust_ccare_csv_for_aztable(raw_df)


def extract_prep_episode_data(extract_start_date, extract_end_date
                      # , active_clients_start_date
                      # , active_clients_end_date
                      , fname):
  
  raw_df =  read_and_prep_csv(f"NSW_CSV/{fname}")
  # get_data('MDS',extract_start_date, extract_end_date, f"./data/in/{fname}.parquet", cache=True)
  
  if isinstance(raw_df, type(None)) or raw_df.empty:
    logger.error("No data found. Exiting.")
    exit(1)
  processed_df = prep_dataframe_episodes(raw_df)
  
  processed_filepath = f"./data/processed/{fname}.parquet"
  # cache the processed data
  write_parquet(processed_df, processed_filepath) # don't force overwrite
  logger.info(f"Done saving processed data to {processed_filepath}")  

  return processed_df

     



import os
import pandas as pd

# List of column names in the CSV
column_names = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION', 'PMSEpisodeID', 'PMSPersonID', 'DOB', 'DOB STATUS', 'SEX', 'COUNTRY OF BIRTH', 'INDIGENOUS STATUS', 'PREFERRED LANGUAGE', 'SOURCE OF INCOME', 'LIVING ARRANGEMENT', 'USUAL ACCOMMODATION', 'CLIENT TYPE', 'PRINCIPAL DRUG OF CONCERN', 'PDCSubstanceOfConcern', 'ILLICIT USE', 'METHOD OF USE PRINCIPAL DRUG', 'INJECTING DRUG USE', 'SETTING', 'CommencementDate', 'POSTCODE', 'SOURCE OF REFERRAL', 'MAIN SERVICE', 'EndDate', 'END REASON', 'REFERRAL TO ANOTHER SERVICE', 'FAMILY NAME', 'GIVEN NAME', 'MIDDLE NAME', 'TITLE', 'SLK', 'MEDICARE NUMBER', 'PROPERTY NAME', 'UNIT FLAT NUMBER', 'STREET NUMBER', 'STREET NAME', 'SUBURB']
# >DATS_NSW All MonthlyForAutomation
# ESTABLISHMENT_IDENTIFIER
#            , GEOGRAPHICAL_LOCATION
#            , EPISODE_ID
#            , PERSON_ID
#            , DOB
#            , DOB_STATUS
#            , SEX
#            , COUNTRY_OF_BIRTH
#            , INDIGENOUS_STATUS
#            , PREFERRED_LANGUAGE
#            , SOURCE_OF_INCOME
#            , LIVING_ARRANGEMENT
#            , USUAL_ACCOMMODATION
#            , CLIENT_TYPE
#            , PRINCIPAL_DRUG_OF_CONCERN
#            , SPECIFY_DRUG_OF_CONCERN
#            , ILLICIT_USE
#            , METHOD_OF_USE_PRINCIPAL_DRUG
#            , INJECTING_DRUG_USE
#            , SETTING
#            , START_DATE
#            , POSTCODE
#            , SOURCE_OF_REFERRAL
#            , MAIN_SERVICE
#            , END_DATE
#            , END_REASON
#            , REFERRAL_TO_ANOTHER_SERVICE
#            , SLK

# List of columns we care about
columns_of_interest = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION', 'PMSEpisodeID', 'PMSPersonID', 'PDCSubstanceOfConcern', 'CommencementDate', 'EndDate', 'SLK']


# TODO : check code
def load_and_parse_csv(filepath):
    # Load the CSV
    df = pd.read_csv(filepath, header=None, names=column_names)

    # Select only the columns we care about
    df = df[columns_of_interest]

    df['CommencementDate'] = pd.to_datetime(df['CommencementDate'], format='%d%m%Y')
    df['EndDate'] = pd.to_datetime(df['EndDate'], format='%d%m%Y')    
    return df



# def load_and_parse_episode_csvs(directory):
#     # List to hold dataframes
#     dfs = []
    
#     # Loop over all files in the directory
#     for filename in os.listdir(directory):
#         # Check if the file is a CSV
#         if not filename.endswith('.csv'):
#             continue
#         filepath = os.path.join(directory, filename)
#         try:
#           df = load_and_parse_csv(filepath)
#         except ValueError as e:
#             logger.error(f"Error parsing dates in file {filepath} with error {str(e)}")
#             # logger.error("The problematic row is:")

            
#             continue  # Skip this file and move to the next one

#         # Append the dataframe to the list
#         dfs.append(df)
    
#     # Concatenate all dataframes in the list
#     final_df = pd.concat(dfs, ignore_index=True)

#     return final_df

def load_and_parse_episode_csvs(directory):
    # List to hold dataframes
    dfs = []
    
    # Loop over all files in the directory
    for filename in os.listdir(directory):
        # Check if the file is a CSV
        if not filename.endswith('.csv'):
          continue
        filepath = os.path.join(directory, filename)
        # Load the CSV
        df = pd.read_csv(filepath, header=None, names=column_names)
        # Select only the columns we care about
        df = df[columns_of_interest]
        # Try to convert CommencementDate and EndDate columns to datetime format
        try:
            df['CommencementDate'] = pd.to_datetime(df['CommencementDate'], format='%d%m%Y',errors='coerce')
            df['EndDate'] = pd.to_datetime(df['EndDate'], format='%d%m%Y', errors='coerce')
        except ValueError as e:
            print(f"Error parsing dates in file {filename} with error {str(e)}")
            print("The problematic row is:")
            print(df.iloc[-1])
            continue  # Skip this file and move to the next one
        # Append the dataframe to the list
        dfs.append(df)
    
    # Concatenate all dataframes in the list
    final_df = pd.concat(dfs, ignore_index=True)

    return final_df
