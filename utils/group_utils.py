import numpy as np

def chrono_rank_within_clientgroup(df1):#, col:str='AssessmentDate'):#, ascending=True):
  df = df1.copy()
  g = df.groupby('SLK')  
  df.loc[:,'survey_rank'] = g['AssessmentDate'].rank(method='min') 
  return df

def range_average(range_str:str, separator:str):
  two_ints = range_str.split(separator)
  return (int(two_ints[0])+int(two_ints[1]))/2


# group_utils.py
def getrecs_w_min_numvals_forcol(df, col:str, min_num_vals=3):# ->pd.DataFrame:

  dfsds = df[ df[col].notna() ]       # exclude rows with NaN
  g = dfsds.groupby('SLK')

  not_all_zeros_df = g.filter(lambda x: x[col].count() >= min_num_vals )  # only those SLK who have at least 3 values
  
  return not_all_zeros_df

def first_n_medians(g, col:str, min_atoms: int):
  nth_medians = [np.median(g.nth(i).loc[:, col]) for i in range(min_atoms) ]
  return nth_medians
    
