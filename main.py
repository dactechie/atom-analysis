import os
import json
import pandas as pd

from data_prep import define_all_categories
from get_data import get_results
from utils.dtypes import convert_to_datetime
from utils.df_ops import concat_drop_parent, \
  drop_columns_byname_regex, \
    expand_nested_dictlist_firstelem, read_parquet, drop_fields


def prep(df):
  df = drop_columns_byname_regex(df) # remove *Goals notes, so do before PDC step (PDCGoals dropdown)
  df = expand_nested_dictlist_firstelem(df,'PDC') # (df,'ODC') # only takes the first ODC   
  convert_to_datetime(df,'AssessmentDate')
  return df

def get_surveydata_expanded(df:pd.DataFrame) -> pd.DataFrame:
  # https://dschoenleber.github.io/pandas-json-performance/
  df_surveydata = df['SurveyData'].apply(json.loads)
  df_surveydata_expanded:pd.DataFrame =  pd.json_normalize(df_surveydata.tolist(), max_level=1)
  df_final  = concat_drop_parent(df, df_surveydata_expanded, 'SurveyData')
  return df_final


def prep_dataframe(df:pd.DataFrame):
  df1 = drop_fields(df,['Program']) # because Program is in SurveyData
 
  df2 = get_surveydata_expanded(df1)
  df3 = drop_fields(df2,['ODC'])
  
  df4 = prep(df3) # removes rows without PDC

  # df.PDCAgeFirstUsed[(df.PDCAgeFirstUsed.notna()) & (df.PDCAgeFirstUsed != '')].astype(int)
 # "Expected bytes, got a 'int' object", 'Conversion failed for column PDCAgeFirstUsed with type object'
  df5 = drop_fields(df4, ['PDCAgeFirstUsed',\
                           'PrimaryCaregiver','Past4WkAodRisks']) 
  # 'cannot mix list and non-list, non-null values', 
  # 'Conversion failed for column PrimaryCaregiver, Past4WkAodRisks with type object')


 
  # df_final  = concat_drop_parent(df, df2, 'SurveyData')
  df_result  = define_all_categories(df5) # do after concat b/c: category for Program which is not in SurveyData
  return df_result


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



def main(fname:str, start_date, end_date):
 
  download_filepath = f"./data/in/{fname}"
  processed_filepath =f"./data/out/processed_all_cols_{fname}"

  df = get_data(start_date, end_date, download_filepath, cache=True)
  if df is None or df.empty:
    print("ERROR: No data")
    return None
  
  df = prep_dataframe(df)


  df.to_parquet(f"{processed_filepath}.parquet")

  print(df)
  return df


if __name__ == "__main__":
  start_date = 20200101
  end_date = 20240101
  fname = f"{start_date}_{end_date}"

  df = main(fname, start_date, end_date)






# param paths have no extension
# def read_process_write(infile_path:str, outfile_path):
#   df = read_parquet(f"{infile_path}.parquet")
  
#   # df = prep_dataframe(df)  
#   # df.drop('PrimaryCaregiver',axis=1, inplace=True) # cannot mix list and non-list, non-null values
#   # df.drop('Past4WkAodRisks',axis=1, inplace=True) # cannot mix list and non-list, non-null values

#   df = get_surveydata_expanded(df)
#   drop_field(df,'ODC')
#   # drop_field(df,'SurveyData')
#   



  # df = pd.read_parquet(f"{download_filepath}.parquet")
  # processed_filepath ="processed"
  

  #     # df.PDCGoals.dtype has : 'Maintain Current Level of Use'
  #     #             and 'Maintain Current level of use'

  # print(df)
  # read_process_write(download_filepath, processed_filepath)


# Exception has occurred: ArrowInvalid
# ('cannot mix list and non-list, non-null values', 'Conversion failed for column PrimaryCaregiver with type object')
#   File "C:\Users\aftab.jalal\dev\atom-analysis\main.py", line 110, in <module>
#     df.to_parquet("processed.parquet")
# pyarrow.lib.ArrowInvalid: ('cannot mix list and non-list, non-null values', 'Conversion failed for column PrimaryCaregiver with type object')


# similar to PDC
# df['Past4WkEngagedInOtheractivities.Paid Work']
# 0                            {'Frequency': 'Not at all'}
# 1                                                    NaN
# 2      {'Frequency': 'Once or twice per week', 'Days'...
# def expand_nested_pdc_skip_nopdc1 (l1:pd.DataFrame):
#   #ones to updte :  
#   # masked_rows=  l1[('PDC' in l1) and l1['PDC'].apply(lambda x: isinstance(x,list))]
#   # pdcs = masked_rows['PDC'].map(lambda x: x[0])
#   # normd_pdc = pd.json_normalize(pdcs.to_list())  # index lost

#   # result = concat_drop_parent(masked_rows, normd_pdc, 'PDC')
#   # return result


#   for index, l1_row in l1.iterrows():
#     if 'PDC' in l1_row and isinstance(l1_row['PDC'],list) :
#       pdc_dict = l1_row['PDC'][0]
#       l1.at[index,'PDCSubstanceOrGambling'] = pdc_dict.get('PDCSubstanceOrGambling')
#       l1.at[index,'PDCMethodOfUse'] = pdc_dict.get('PDCMethodOfUse')
#       l1.at[index,'PDCDaysInLast28'] = pdc_dict.get('PDCDaysInLast28')
#       l1.at[index,'PDCHowMuchPerOccasion'] = pdc_dict.get('PDCHowMuchPerOccasion')
#       l1.at[index,'PDCUnits'] = pdc_dict.get('PDCUnits')
#       l1.at[index,'PDCGoals'] = pdc_dict.get('PDCGoals')
#   return l1


  # import re
  
  # comment_rgex = re.compile("^((?!Comment).)*$", re.IGNORECASE)
  # note_rgex = re.compile("^((?!Note).)*$", re.IGNORECASE)
  # df[:,~df.columns.str.contains('Comment') & ~df.columns.str.contains('Note') & ~df.columns.str.contains('ITSP') ]
 
  # df = df.filter(regex=comment_rgex)

  # df = df.filter(regex=note_rgex)
  

  # col_presuffixes_to_drop = ('Comment','Note')
  # cols_to_keep = [col for col in col_presuffixes_to_drop 
  #                 if col.find(df.columns) == -1 ]
  # return df[cols_to_keep]
  # cols = [c for c in df.columns if c.lower() != 'test']
  # df.drop(df.columns[df.columns.str.contains(regex_str)], axis=1)