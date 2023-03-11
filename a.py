import pandas as pd
# from pandas.core.groupby import DataFrameGroupBy
# import numpy as np
from utils.fields_process import  prep # filter_to_numeric
from utils.group_utils import  limit_pkey_num_assessments, get_clean_for_col, first_n_medians
ESSENTIAL_FIELDS = ['PartitionKey', 'Program', 'AssessmentDate']



def get_data_csv(filename_str) -> pd.DataFrame:
  df = pd.read_csv(filename_str
           #,nrows=2000
           ,usecols=[
            *ESSENTIAL_FIELDS,
              'Staff', 'SDS_Score',
              'PDC.PDCSubstanceOrGambling',
            'PDC.PDCDaysInLast28',
            'PDC.PDCHowMuchPerOccasion',
            'Past4WkPhysicalHealth',
            'Past4WkMentalHealth',
            'Past4WkQualityOfLifeScore'
            ]
          #, na_values=[]
          , parse_dates=['AssessmentDate'], dayfirst=True    
          #  , encoding='iso-8859-1',       # ,  warn_bad_lines=True #, error_bad_lines=False
       )
  
  df = prep(df)
  return df


if __name__ == "__main__":
  fname = "./data/atom_raw.csv"
  min_assessments_per_client = 3
  df = get_data_csv(fname)
  dfmin3 = limit_pkey_num_assessments(df, min_assessments_per_client)

  col ='SDS_Score'
  col_df = get_clean_for_col(dfmin3, col)

  grp = col_df.groupby('PartitionKey')
  
  arr = first_n_medians(grp, col, min_assessments_per_client)

  print (col_df)

  

