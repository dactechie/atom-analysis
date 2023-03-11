import pandas as pd
from data_config import rename_cols

def remove_outliers(df: pd.DataFrame):
  Q1 = df.quantile(0.25)
  Q3 = df.quantile(0.75)
  IQR = Q3 - Q1

  return df[((df < (Q1 - 1.5 * IQR)) & (df > (Q3 + 1.5 * IQR))).all(axis=1)]




def remove_bad_data(df) -> pd.DataFrame:
  df = df [ ~ (df['Program'] == 'TEST') ] 
  df = df[~df['Staff'].isin( ['Aftab.Jalal', 'Corrina.Trimarchi'])]
  return df


def prep(df: pd.DataFrame) -> pd.DataFrame:
  df = remove_bad_data(df)
  df = df.rename(columns=rename_cols)
  df = df.sort_values(by="AssessmentDate")
  return df