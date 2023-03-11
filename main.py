# import json
import pandas as pd
from azutil.az_tables_query import SampleTablesQuery, get_json_result


def get_results() -> list[dict]:
    stq = SampleTablesQuery()

    fields = [u"PartitionKey", u"RowKey", u"SurveyData"]
    assessment_date_limits = {u"lower": 20230101, u"upper": 20230310}
    name_filter = u"AssessmentDate ge @lower and AssessmentDate lt @upper"
    results = [
         get_json_result(json_atom) 
         for json_atom in 
         stq.query_atoms(fields, filter_template=name_filter, query_params=assessment_date_limits)
         ]
    return results


def drop_columns_byname_regex(df):
  regex_str = 'Comment|Note'
  df.drop(df.columns[df.columns.str.contains(regex_str)], axis=1)

def convert_to_datetime(df):
  df ['AssessmentDate'] = pd.to_datetime(df['AssessmentDate'])

def concat_drop_parent(df, df2 ,parent_name:str) -> pd.DataFrame:
   return pd.concat([df.drop(parent_name, axis=1), df2], axis=1)

# similar to PDC
# pq_df['Past4WkEngagedInOtheractivities.Paid Work']
# 0                            {'Frequency': 'Not at all'}
# 1                                                    NaN
# 2      {'Frequency': 'Once or twice per week', 'Days'...
def expand_nested_pdc_skip_nopdc (l1:pd.DataFrame):
  #ones to updte :  
  masked_rows=  l1[('PDC' in l1) and l1['PDC'].apply(lambda x: isinstance(x,list))]
  pdcs = masked_rows['PDC'].map(lambda x: x[0])
  normd_pdc = pd.json_normalize(pdcs)  # index lost

  result = concat_drop_parent(masked_rows, normd_pdc, 'PDC')
  return result


  # for index, l1_row in l1.iterrows():
  #   if 'PDC' in l1_row and isinstance(l1_row['PDC'],list) :
  #     pdc_dict = l1_row['PDC'][0]
  #     l1.at[index,'PDCSubstanceOrGambling'] = pdc_dict.get('PDCSubstanceOrGambling')
  #     l1.at[index,'PDCMethodOfUse'] = pdc_dict.get('PDCMethodOfUse')
  #     l1.at[index,'PDCDaysInLast28'] = pdc_dict.get('PDCDaysInLast28')
  #     l1.at[index,'PDCHowMuchPerOccasion'] = pdc_dict.get('PDCHowMuchPerOccasion')
  #     l1.at[index,'PDCUnits'] = pdc_dict.get('PDCUnits')
  # return l1


def prep(df):
  df = expand_nested_pdc_skip_nopdc(df)
  drop_columns_byname_regex(df)
  convert_to_datetime(df)  
  return df

def read_prep(file_path:str):
  import json
  df = pd.read_parquet(file_path)
  # https://dschoenleber.github.io/pandas-json-performance/
  df_surveydata = df['SurveyData'].apply(json.loads)
  df_surveydata_expanded:pd.DataFrame =  pd.json_normalize(df_surveydata, max_level=1)
  df2 = prep(df_surveydata_expanded) # removes rows without PDC
  df_final  = concat_drop_parent(df, df2, 'SurveyData') 
  return df_final


def get_and_write_to_pq():
  #
  # get from ATOM Azure DB and save to disk
  #
  results = get_results()
  df = pd.DataFrame.from_records(results)
  df.to_parquet("a.parquet")

# param paths have no extension
def read_process_write(infile_path:str, outfile_path):
  pq_df = read_prep(f"{infile_path}.parquet")
  pq_df.drop('PrimaryCaregiver',axis=1, inplace=True) # cannot mix list and non-list, non-null values
  pq_df.drop('Past4WkAodRisks',axis=1, inplace=True) # cannot mix list and non-list, non-null values
  pq_df.to_parquet(f"{outfile_path}.parquet")

if __name__ == "__main__":
  download_filepath = "a"
  processed_filepath ="processed"
  read_process_write(download_filepath, processed_filepath)


  



# Exception has occurred: ArrowInvalid
# ('cannot mix list and non-list, non-null values', 'Conversion failed for column PrimaryCaregiver with type object')
#   File "C:\Users\aftab.jalal\dev\atom-analysis\main.py", line 110, in <module>
#     pq_df.to_parquet("processed.parquet")
# pyarrow.lib.ArrowInvalid: ('cannot mix list and non-list, non-null values', 'Conversion failed for column PrimaryCaregiver with type object')
