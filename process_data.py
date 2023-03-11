## %%

import pandas as pd
# from pandas.core.groupby import DataFrameGroupBy
import numpy as np
from fields_process import filter_to_numeric, prep

# Effective Pandas https://www.youtube.com/watch?v=zgbUk90aQ6A
def define_categories(df):
  return df
  # dt12 = pd.CategoricalDtype(cat_q12to15_17_18, ordered=True)
  # adom[q16] =  pd.Series(adom[q16], dtype=dt16)


def count_client_assessments(df: pd.DataFrame) -> dict:
  return {}

def convert_types(df) -> pd.DataFrame:  
  return df
  
  # clean specific column for analysis
  # df.assign(PDCDaysInLast28=df[['PDC.PDCDaysInLast28']].dropna().astype('int8'))
  # .drop(columns=['PDC.PDCDaysInLast28'])  

# first , second and nth of each client
# https://stackoverflow.com/questions/56787717/select-middle-value-for-nth-rows-in-python
  # # df2 = df.groupby(np.arange(len(df))//N).apply(lambda x : x.iloc[len(x)//2])
def nthpctile_grpclients(atom_client_groups, nth=0.8):
  # fmean = atom_client_groups.nth(0).mean()
  # smean = atom_client_groups.nth(1).mean()
  # lmean = atom_client_groups.nth(-1).mean()  
  first = atom_client_groups.nth(0).quantile(nth)
  second = atom_client_groups.nth(1).quantile(nth)
  last = atom_client_groups.nth(-1).quantile(nth)
  return first, second, last

##  validated 'ANY' non-zero filter
## valid = g.filter(lambda x: any(x['PDCHowMuchPerOccasion']) > 0)
##  194: uniquPK = pd.unique(valid.loc[valid['PDCHowMuchPerOccasion']==0]['PartitionKey'])
##  194 : (unique list of Pks where at least one item is > 0 ) len(pd.unique(valid[valid['PartitionKey'].isin(uniquPK) & valid['PDCHowMuchPerOccasion']>0]['PartitionKey'])) 
##
##

# Effective Pandas https://www.youtube.com/watch?v=zgbUk90aQ6A time: 50 minute

def debug_intermediate_df(df, var_name):
  globals()[var_name] = df
  return df

# df is already sorted by AssessmentDate
def group_filter_forstats(df: pd.DataFrame, col:str, min_num_assessments:int = 3): #-> DataFrameGroupBy:
  # measure = df[['PartitionKey', col]]
  # g = measure.groupby('PartitionKey')  
  clean_df = df[ ~df[col].isna() ]
  # df[col].dropna()
  
  # TODO :
  # if more NAs than data, then remove SLK from group ?

  g = clean_df.groupby('PartitionKey')
  counts =  g['PartitionKey'].count()
  gt2_ATOM_SLKs = counts[ counts >= min_num_assessments ].index.tolist()
  df_gt2 = clean_df [ clean_df['PartitionKey'].isin(gt2_ATOM_SLKs) ]

  # df [df['PartitionKey'].isin((g['PartitionKey'].count()> 5).index)]

  # measure = df_gt2[['PartitionKey', col]]
  # g = measure.groupby('PartitionKey')
  # g:DataFrameGroupBy = df_gt2.groupby('PartitionKey')
  g = df_gt2.groupby('PartitionKey')
  
  not_all_zeros_df = g.filter(lambda x: any(x[col]) > 0) # validated

  g = not_all_zeros_df.groupby('PartitionKey')
  return g

  # counts =  g['PartitionKey'].count()
# df is already sorted by AssessmentDate

def first_last(g, col:str):
  first = np.mean(g.nth(0).loc[:, col])
  last =np.mean(g.nth(-1).loc[:,col])
  avg_days_between: float = np.mean(g.nth(-1)['AssessmentDate'] - g.nth(0)['AssessmentDate']).days  # type: ignore

  return first, last, avg_days_between

# def data_howmuch():
#   df = pd.read_csv("./data.csv"
#             ,nrows=2000
#            ,usecols=[
#             'PartitionKey',
#             'Program',  'AssessmentDate', 
#             'PDC.PDCHowMuchPerOccasion',
#             ]
#   )#, parse_dates=['AssessmentDate'], dayfirst=True)

#   df = prep(df)
#   df = howmuch(df) # PDCDaysInLast28
#   return df

#https://stackoverflow.com/questions/23199796/detect-and-exclude-outliers-in-a-pandas-dataframe
# answered Sep 20, 2016 at 10:40
# Oleg N. Osychenko
def remove_outliers(df: pd.DataFrame, column:str):
  df_sub = df.loc[:, column]
  
  iqr = df_sub.quantile(0.75) - df_sub.quantile(0.25)
  
  # iqr filter: within 2.22 IQR (equiv. to z-score < 3)
  lim = np.abs((df_sub - df_sub.median()) / iqr) < 2.22
  

  # replace outliers with nan
  df.loc[:, column] = df_sub.where(lim, np.nan)
  
  df.dropna(subset=[column], inplace=True)
  
  return df

def get_stats_for(df:pd.DataFrame, column:str , min_num_assessments:int):
  g = group_filter_forstats(df, column, min_num_assessments)

  # df = remove_outliers(df, column) 
  
  # mean
  first, last, avg_days_btw = first_last(g, column)
  #median
  #   g.nth(0).loc[:, column].median() # 3.0
  # g.nth(-1).loc[:, column].median() # 2.0
  return first, last, avg_days_btw, len(g)

def run_print_stats(df, columns, min_num_assessments):
  for column in columns:
    first, last, avg_days_btw, contributing = get_stats_for(df, column, min_num_assessments)
    print(f"\t ----- Metric: {column}  ------")
    print(f"\t Mean of first values {first:.2f} , mean of last values: {last:.2f}. Contributing Clients {contributing}")
    print(f"\t Average Days between first and last: {avg_days_btw}")
    # print("\n")  

# =XLOOKUP(B3,[AllProgramsMatch.xlsx]AllNewClean!$A:$A,[AllProgramsMatch.xlsx]AllNewClean!$B:$B)
def main():
  df = pd.read_csv("./SENSW.csv",usecols=['PartitionKey', 'Program',
           'AssessmentDate','Staff','SurveyName',
           'ClientType', 'IndigenousStatus','PDC.PDCDaysInLast28', 
           'PDC.PDCHowMuchPerOccasion', 'Past4WkPhysicalHealth', 
           'Past4WkMentalHealth', 'Past4WkQualityOfLifeScore'] ,
          dtype={'Program': 'category', 'ClientType': 'category', 
            'IndigenousStatus': 'category', 'Staff': 'category', 
            'SurveyName':'category'}, 
          parse_dates=['AssessmentDate'],dayfirst=True)
  #df.memory_usage(deep=True)
  # (df[cols].select_dtypes(float).describe()) 
  df = prep(df)

  # column = 'PDCHowMuchPerOccasion'
  #df = filter_to_numeric(df, column) # PDCDaysInLast28
  min_num_assessments = 2
  # print("\t\t Increasing score => Good ")
  # run_print_stats(df, ['Past4WkPhysicalHealth', 'Past4WkMentalHealth', 'Past4WkQualityOfLifeScore'], min_num_assessments)

  print("\t\t Decreasing score => Good ")
  cols = [ 'PDCDaysInLast28', 'PDCHowMuchPerOccasion']
  run_print_stats(df, cols, min_num_assessments)

  # df2 = remove_outliers(df,column)
  # g2 = group_filter_forstats(df2, column, min_num_assessments)
  # g2.nth(0).loc[:, column].median()
  # print(df.head())

if __name__ == "__main__":
  main()

## %%




#g.nth(-1).loc[:,'PDCHowMuchPerOccasion'] - g.nth(0).loc[:,'PDCHowMuchPerOccasion']

# np.mean(g.nth(0).loc[:,'PDCHowMuchPerOccasion'])
# 8.063785394932935
# np.mean(g.nth(-1).loc[:,'PDCHowMuchPerOccasion'])
# 7.633084947839047
 # g
 # np.mean(g.nth(-1)['PDCHowMuchPerOccasion'])

# gt7 = g.filter(lambda x: len(x) > 7)
# gt7.loc[:,'PDCHowMuchPerOccasion'].quantile(0.9)
# h = np.array(gt7.loc[:,'PDCHowMuchPerOccasion'])
# h
# array([25., 10.,  2.,  2.,  2.,  2.,  2.,  2.])
# np.percentile(h,80,method='normal_unbiased')
# 9.800000000000004
# np.percentile(h,80,method='linear')
# 6.800000000000004

# np.percentile([1,2,3,5,10],80)
# 6.000000000000001
# np.percentile([1,2,3,5,10],80,method='normal_unbiased')
# 7.875000000000001

# def convert_types(df) -> pd.DataFrame:
#   dtype={
#             'PartitionKey': 'string',
#           'RowKey': 'string',
#           'Program': 'string',
#           'Staff': 'string',
#           # 'AssessmentDate': 'date',
#           'SurveyName': 'string',
#           # 'ClientType': 'string',
#           # 'IndigenousStatus': 'string',
#           # 'PDC.PDCSubstanceOrGambling': 'string',
#           # 'PDC.PDCMethodOfUse': 'string',
#           # 'PDC.PDCDaysInLast28': 'int',
#           # 'PDC.PDCUnits': 'string',
#           # 'PDC.PDCHowMuchPerOccasion': 'float',
#           # 'HaveYouEverInjected': 'string',
#           # 'SDSIsAODUseOutOfControl': 'int',
#           # 'SDSDoesMissingFixMakeAnxious': 'int',
#           # 'SDSHowMuchDoYouWorryAboutAODUse': 'int',
#           # 'SDSDoYouWishToStop': 'int',
#           # 'SDSHowDifficultToStopOrGoWithout': 'int',
#   }          
#   return df



  # df = pd.read_csv("./SENSW.csv"
  #          #,nrows=2000
  #          ,usecols=[
  #           'PartitionKey',
  #          # 'RowKey',
  #           'Program', 'AssessmentDate',
  #             'Staff',
  #           #  'SurveyName', 'ClientType', 'IndigenousStatus',
  #           # 'PDC.PDCSubstanceOrGambling',
  #           # 'PDC.PDCMethodOfUse',
  #           'PDC.PDCDaysInLast28',
  #           # 'PDC.PDCUnits',
  #           'PDC.PDCHowMuchPerOccasion',
  #           'Past4WkPhysicalHealth',
  #           'Past4WkMentalHealth',
  #           'Past4WkQualityOfLifeScore',
  #           # 'Past4WkHowOftenPhysicalHealthCausedProblems',
  #           # 'Past4WkHowOftenMentalHealthCausedProblems',
  #           # 'HaveYouEverInjected',
  #           # 'SDSIsAODUseOutOfControl',
  #           # 'SDSDoesMissingFixMakeAnxious',
  #           # 'SDSHowMuchDoYouWorryAboutAODUse',
  #           # 'SDSDoYouWishToStop',
  #           # 'SDSHowDifficultToStopOrGoWithout',
  #           ]
  #     # , na_values=[]
  #   , parse_dates=['AssessmentDate'], dayfirst=True
    
  #         #  , encoding='iso-8859-1', 
  #           # ,  warn_bad_lines=True #, error_bad_lines=False
  #             )