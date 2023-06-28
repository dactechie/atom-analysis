
from utils.io import get_data
from data_prep import prep_dataframe


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