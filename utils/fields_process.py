
import pandas as pd
import numpy as np
from data_config import rename_cols
from utils.group_utils import range_average


def remove_bad_data(df) -> pd.DataFrame:
  df = df [ ~ (df['Program'] == 'TEST') ] 
  # df = df[~df['Staff'].isin( ['Aftab.Jalal', 'Corrina.Trimarchi'])]
  return df

def prep(df: pd.DataFrame) -> pd.DataFrame:
  df = remove_bad_data(df)
  df = df.rename(columns=rename_cols)
  df = df.sort_values(by="AssessmentDate")
  return df


def missing_pc(df: pd.DataFrame) -> pd.DataFrame:
  percent_missing = df.isnull().sum() * 100 / len(df)
  missing_value_df = pd.DataFrame({'col_name': df.columns,
                                  'percent_missing': percent_missing})
  missing_value_df = missing_value_df[missing_value_df['percent_missing'] > 0]
  missing_value_df.sort_values('percent_missing', inplace=True)
  return missing_value_df


def calc_rangeavg(df:pd.DataFrame, column:str, range_separator='-'):
  df_r  = df.copy()
  mask = df_r[column].str.contains(range_separator)
  # TODO : UNABLE TO UPDATED THE MASK

  if any(mask[mask==True]):
    df_r.loc[mask, column]= df_r.loc[mask, column].apply(lambda x: range_average(x,range_separator))
  
  return df_r

def get_numericavg_forrange(df: pd.DataFrame, column:str) -> pd.DataFrame:
  df_r = df[ (~ df[column].isna())  & ~( df[column] == 'Other')]
  
  df_r = calc_rangeavg (df_r, column)
  df_r = calc_rangeavg (df_r, column,'to')
  
  df_r.loc[:,column] = df_r.loc[:,column].astype(np.float64)
  return df_r